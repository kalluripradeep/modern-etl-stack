"""
Batch ETL Pipeline: PostgreSQL (Source) -> MinIO (Storage) -> PostgreSQL (Dest) -> dbt
Generic ingestion for all source tables into the Bronze Lakehouse layer.
"""

import io
import os
import sys
import glob
import tempfile
import logging
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Airflow 3.3+ no longer puts the dags folder on sys.path during parsing
sys.path.insert(0, str(Path(__file__).parent))
from pipeline_config import load_manifest, raw_ddl  # noqa: E402

log = logging.getLogger(__name__)


def check_source_schema(source_hook, table_name, needed):
    """Fail with a legible message if the source lacks a manifested column.

    The extract selects the manifest's column list verbatim. When the source
    predates the manifest the query dies inside pandas with a bare

        psycopg2.errors.UndefinedColumn: column "name" does not exist

    naming one column, buried under a SQLAlchemy traceback, and offering no
    remedy — the same trap simulate_live_traffic.py already guards against.
    Checking up front names every missing column at once and says what to do.
    """
    rows = source_hook.get_records(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = %s",
        parameters=(table_name,),
    )
    present = {r[0] for r in rows}
    if not present:
        raise AirflowException(
            f"Source table '{table_name}' does not exist. Seed the source "
            f"first: make seed (or re-run k8s/deploy.sh and answer y when it "
            f"offers to seed)."
        )

    missing = [c for c in needed if c not in present]
    if missing:
        raise AirflowException(
            f"Source table '{table_name}' is missing {', '.join(missing)}, "
            f"which airflow/dags/config/pipelines.yml expects.\n"
            f"  present: {', '.join(sorted(present))}\n"
            f"This means the source was seeded by an older version of "
            f"sample-data/generate_ecommerce.py. Re-seed to bring it in line "
            f"(this drops and recreates the source tables, so the Debezium "
            f"slot and the ClickHouse mirror restart from empty):\n"
            f"  make seed\n"
            f"On Kubernetes, re-run bash k8s/deploy.sh and answer y when it "
            f"offers to seed."
        )

# Single source of truth for tables: airflow/dags/config/pipelines.yml
MANIFEST = load_manifest()
TABLES_CONFIG = MANIFEST['tables']

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dbt_k8s_path = Path("/opt/airflow/dags/repo/dbt")
dbt_docker_path = Path("/opt/airflow/dbt")

if dbt_k8s_path.exists():
    DBT_PROJECT_PATH = dbt_k8s_path
elif dbt_docker_path.exists():
    DBT_PROJECT_PATH = dbt_docker_path
else:
    DBT_PROJECT_PATH = Path(__file__).parent.parent.parent / "dbt"

profile_config = ProfileConfig(
    profile_name="modern_etl",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="dest_postgres",
        profile_args={"schema": "int"},
    ),
)

dag = DAG(
    'ingest_source_to_bronze',
    default_args=default_args,
    description='Extract all source tables from postgres to data lake (Bronze)',
    # Hourly by default — extraction is incremental (high-water mark on the
    # cursor column), so a shorter cadence just means fewer rows per run and
    # fresher warehouse data. Override with INGEST_SCHEDULE if an environment
    # needs a different cadence (any Airflow schedule expression).
    schedule=os.environ.get('INGEST_SCHEDULE', '@hourly'),
    catchup=False,
    # Never let a slow run overlap the next one: concurrent runs would race
    # on the same raw tables and high-water mark.
    max_active_runs=1,
    # A run that never reaches a terminal state holds the single active slot
    # for good. #144 hit exactly that: a scheduled run from 2026-08-15 sat in
    # "running" for nine days after its task hit
    #   state mismatch ... Executor reported ... finished with state failed,
    #   but the task instance's state attribute is queued
    # so every later run stayed queued behind it and the lakehouse silently
    # stopped being built. Nothing surfaced it, because a queued run is not an
    # error -- there is simply no error anywhere to find.
    #
    # dagrun_timeout makes the scheduler fail such a run instead of waiting on
    # it forever. Two hours is well clear of a real run (minutes) while still
    # clearing a zombie the same day.
    dagrun_timeout=timedelta(hours=2),
    tags=['etl', 'bronze', 'multi-table'],
)


def extract_and_load_table(table_name, **kwargs):
    """
    Generic extraction and loading function for any source table.
    """
    config = TABLES_CONFIG[table_name]
    pk = config['primary_key']
    columns = list(config['columns'].keys())
    cursor_col = config.get('cursor_column')
    logical_date = kwargs.get('logical_date', datetime.now())
    date_prefix = logical_date.strftime("%Y/%m/%d")
    # Bronze objects stay in a daily folder (that is what the Spark jobs read),
    # but the filename carries the run timestamp. Without it every run of the
    # same day writes part-00000.parquet and overwrites the previous one —
    # harmless at @daily, silent data loss for the lakehouse at any faster
    # cadence, since each incremental run only holds its own new rows.
    # A retry of the same run reuses its stamp and overwrites itself, which
    # is the idempotent behaviour we want.
    run_stamp = logical_date.strftime("%Y%m%dT%H%M%S")

    chunk_size = int(os.environ.get('ETL_CHUNK_SIZE', 10000))
    bucket_name = 'bronze'

    source_hook = PostgresHook(postgres_conn_id='source_postgres')
    dest_hook = PostgresHook(postgres_conn_id='dest_postgres')
    s3_hook = S3Hook(aws_conn_id='minio_s3')

    if not s3_hook.check_for_bucket(bucket_name):
        s3_hook.create_bucket(bucket_name=bucket_name)

    # 1. Ensure target table exists in DWH
    dest_hook.run("CREATE SCHEMA IF NOT EXISTS raw;")
    dest_hook.run(raw_ddl(table_name, config))

    # 2. Extract from Source (Incremental CDC Logic)
    # The cursor is filtered on as well as selected, so it has to exist too.
    # dict.fromkeys rather than set(): the cursor column is almost always in
    # `columns` too, and a plain concatenation reported it twice --
    #   "is missing first_name, last_name, ..., updated_at, updated_at"
    # which reads like two different problems. Order is kept so the message
    # follows the manifest.
    needed = list(dict.fromkeys(columns + ([cursor_col] if cursor_col else [])))
    check_source_schema(source_hook, table_name, needed)
    cols_str = ",".join(columns)

    # 2a. Fetch the high-water mark (MAX cursor) from the Destination staging DB
    max_date = None
    if cursor_col:
        try:
            # identifiers come from the manifest, never from user input
            records = dest_hook.get_records(
                f"SELECT MAX({cursor_col}) FROM raw.{table_name}_source"  # nosec B608
            )
            if records and records[0] and records[0][0]:
                max_date = records[0][0]
        except Exception as e:
            log.warning(f"Could not fetch high-water mark for {table_name}, doing full refresh. Error: {e}")

    # 2b. Build dynamic Source query
    if max_date:
        log.info(f"CDC Active: Extracting {table_name} where {cursor_col} > '{max_date}'")
        formatted_date = max_date.strftime('%Y-%m-%d %H:%M:%S.%f')
        query = f"SELECT {cols_str} FROM {table_name} WHERE {cursor_col} > '{formatted_date}' ORDER BY {pk}"  # nosec B608
    else:
        log.info(f"CDC Inactive: Performing full extraction for {table_name}")
        query = f"SELECT {cols_str} FROM {table_name} ORDER BY {pk}"  # nosec B608
    source_engine = source_hook.get_sqlalchemy_engine()

    with tempfile.TemporaryDirectory() as tmpdirname:
        total_rows = 0
        log.info(f"Starting extraction for table: {table_name}")
        
        for chunk_idx, df_chunk in enumerate(pd.read_sql(query, source_engine, chunksize=chunk_size)):
            # Skip empty chunks. pd.read_sql(chunksize=...) yields one empty
            # DataFrame when the query matches nothing, which an incremental
            # run with no new rows does constantly. Writing that produced a
            # 0-row parquet whose every column is null-typed, and pyarrow has
            # no dtype to infer from.
            #
            # Spark then reads such a file as INT for every column -- email
            # INT, created_at INT -- and merging that with a real partition
            # fails outright:
            #   CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE "INT" and "BIGINT"
            #   Parquet column cannot be converted ... Expected: int, Found: INT64
            # mergeSchema does not rescue it; the degenerate schema is the
            # problem, not the merge.
            #
            # This was harmless while each Spark job read only its own day.
            # Once #151 made them read every retained partition, one empty file
            # anywhere under the prefix broke the whole table's transform. The
            # file has no business existing either way: Bronze is a record of
            # rows extracted, and there were none.
            if df_chunk.empty:
                continue
            out_path = os.path.join(tmpdirname, f'part-{chunk_idx:05d}.parquet')
            df_chunk.to_parquet(out_path, index=False, compression='snappy', coerce_timestamps='us', allow_truncated_timestamps=True)
            total_rows += len(df_chunk)
        
        log.info(f"Extracted {total_rows} total rows for {table_name}")

        chunk_files = sorted(glob.glob(os.path.join(tmpdirname, 'part-*.parquet')))
        if not chunk_files:
            log.warning(f"No data extracted for {table_name}. Exiting.")
            return

        # 3. Upload to S3 and Sync to DWH
        total_loaded = 0
        with dest_hook.get_conn() as dest_conn:
            with dest_conn.cursor() as pg_cursor:
                for idx, file_path in enumerate(chunk_files):
                    df_chunk = pd.read_parquet(file_path)
                    
                    # Basic validation
                    if df_chunk[pk].isnull().any():
                        raise ValueError(f"Data quality error: Null PKs in {table_name} at {file_path}")

                    # Upload to MinIO
                    object_name = f'{table_name}_source/{date_prefix}/part-{run_stamp}-{idx:05d}.parquet'
                    s3_hook.load_file(filename=file_path, key=object_name, bucket_name=bucket_name, replace=True)

                    # Upsert into DWH
                    buffer = io.StringIO()
                    df_chunk.to_csv(buffer, index=False, header=False)
                    buffer.seek(0)

                    stage_table = f"{table_name}_stage"
                    pg_cursor.execute(f"CREATE TEMP TABLE IF NOT EXISTS {stage_table} (LIKE raw.{table_name}_source) ON COMMIT PRESERVE ROWS")
                    pg_cursor.execute(f"TRUNCATE {stage_table}")
                    
                    copy_sql = f"COPY {stage_table} ({cols_str}) FROM STDIN WITH CSV"  # nosec B608
                    # psycopg2 and psycopg3 disagree here, and which one is
                    # installed is not ours to choose: the postgres provider
                    # moved to psycopg3 in its 6.x line, where copy_expert does
                    # not exist at all --
                    #   AttributeError: 'Cursor' object has no attribute 'copy_expert'
                    # -- and requirements-airflow.txt floors that provider
                    # without capping it, so a rebuild picks up the new major on
                    # its own. Exactly the time bomb the pyspark pin in that
                    # file already warns about, on a different dependency.
                    #
                    # Asking the cursor what it supports works under either and
                    # needs no pin, which is better than freezing the provider
                    # and forgoing its fixes.
                    if hasattr(pg_cursor, "copy_expert"):
                        pg_cursor.copy_expert(copy_sql, buffer)
                    else:
                        with pg_cursor.copy(copy_sql) as copy:
                            copy.write(buffer.read())
                    
                    update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in config['update_columns']])
                    # all identifiers come from the manifest, never from user input
                    upsert_sql = f"""
                        INSERT INTO raw.{table_name}_source
                        SELECT * FROM {stage_table}
                        ON CONFLICT ({pk}) DO UPDATE SET
                            {update_set}
                    """  # nosec B608
                    pg_cursor.execute(upsert_sql)
                    dest_conn.commit()
                    total_loaded += len(df_chunk)
                
        log.info(f"Successfully synced {total_loaded} rows for {table_name}")

    _emit_metric(f'etl.rows_synced.{table_name}', total_loaded)


def _emit_metric(name, value):
    """Best-effort statsd gauge, scraped by Prometheus via statsd-exporter.
    Metrics are optional — never fail a task because monitoring is down."""
    try:
        try:
            from airflow.sdk.observability.stats import Stats
        except ImportError:  # older Airflow
            from airflow.stats import Stats
        Stats.gauge(name, value)
    except Exception as e:  # pragma: no cover - monitoring must not break ETL
        log.warning(f"Could not emit metric {name}: {e}")


def emit_data_quality_metrics(**kwargs):
    """Publish source-freshness and revenue-anomaly signals for alerting."""
    from datetime import timezone

    dest_hook = PostgresHook(postgres_conn_id='dest_postgres')

    # Freshness: seconds since the newest row per table (cursor column)
    for name, table in TABLES_CONFIG.items():
        cursor_col = table.get('cursor_column')
        if not cursor_col:
            continue
        try:
            rec = dest_hook.get_first(
                f"SELECT EXTRACT(EPOCH FROM (now() - MAX({cursor_col}))) "  # nosec B608
                f"FROM raw.{name}_source"
            )
            if rec and rec[0] is not None:
                _emit_metric(f'etl.freshness.{name}', float(rec[0]))
        except Exception as e:
            log.warning(f"Freshness metric failed for {name}: {e}")

    # Revenue anomaly: z-score of the latest completed day vs the prior 30
    try:
        rows = dest_hook.get_records("""
            SELECT order_date::date AS d, SUM(total_amount) AS revenue
            FROM raw.orders_source
            WHERE status <> 'cancelled'
            GROUP BY 1 ORDER BY 1 DESC LIMIT 31
        """)
        if rows and len(rows) >= 8:
            values = [float(r[1]) for r in rows if r[1] is not None]
            latest, history = values[0], values[1:]
            mean = sum(history) / len(history)
            var = sum((x - mean) ** 2 for x in history) / len(history)
            std = var ** 0.5
            z = (latest - mean) / std if std > 0 else 0.0
            _emit_metric('etl.revenue_anomaly_z', z)
            log.info(f"Revenue anomaly z-score: {z:.2f} (latest={latest:.2f}, mean={mean:.2f})")
    except Exception as e:
        log.warning(f"Revenue anomaly metric failed: {e}")

    _ = kwargs, timezone  # reserved for future per-run labelling


def prune_bronze(**kwargs):
    """Delete Bronze partitions older than BRONZE_RETENTION_DAYS.

    Bronze is a landing area: once a run has been transformed into Silver and
    the warehouse, the raw extract has served its purpose. Nothing deleted it,
    so an hourly pipeline wrote parquet into MinIO forever -- on a single-node
    cluster that storage is the node's disk, and it is one of the things that
    put the reporting cluster into DiskPressure.

    Set BRONZE_RETENTION_DAYS=0 to disable if you need the full history.
    """
    retention_days = int(os.environ.get('BRONZE_RETENTION_DAYS', '7'))
    if retention_days <= 0:
        log.info("BRONZE_RETENTION_DAYS=0 — retaining all Bronze data")
        return

    cutoff = datetime.utcnow() - timedelta(days=retention_days)
    s3_hook = S3Hook(aws_conn_id='minio_s3')
    bucket = 'bronze'
    if not s3_hook.check_for_bucket(bucket):
        log.info("Bronze bucket does not exist yet — nothing to prune")
        return

    keys = s3_hook.list_keys(bucket_name=bucket) or []
    stale = []
    for key in keys:
        # Layout: <table>_source/YYYY/MM/DD/part-*.parquet
        parts = key.split('/')
        if len(parts) < 4:
            continue
        try:
            written = datetime(int(parts[1]), int(parts[2]), int(parts[3]))
        except (ValueError, IndexError):
            continue  # not a dated partition; leave it alone
        if written < cutoff:
            stale.append(key)

    empties = _empty_parquet_keys(s3_hook, bucket, set(keys) - set(stale))
    if empties:
        # 0-row parquet written before the guard in extract_and_load_table.
        # Every column in one is null-typed, which Spark reads as INT, so a
        # single leftover breaks the transform for that whole table until
        # retention ages it out -- up to BRONZE_RETENTION_DAYS of a broken
        # lakehouse. Cheaper to remove them than to wait.
        s3_hook.delete_objects(bucket=bucket, keys=empties)
        log.info(f"Removed {len(empties)} empty Bronze object(s)")

    if not stale:
        log.info(f"No Bronze objects older than {retention_days} days")
        return

    s3_hook.delete_objects(bucket=bucket, keys=stale)
    log.info(f"Pruned {len(stale)} Bronze objects older than "
             f"{retention_days} days (before {cutoff:%Y-%m-%d})")


def _empty_parquet_keys(s3_hook, bucket, keys):
    """Keys under `bucket` whose parquet holds no rows.

    Reads footers only -- these files are a few KB -- and treats anything it
    cannot parse as "leave alone", since deleting on a read error would turn a
    transient S3 problem into data loss.
    """
    import pyarrow.parquet as pq

    empty = []
    for key in keys:
        if not key.endswith('.parquet'):
            continue
        try:
            body = s3_hook.get_key(key, bucket_name=bucket).get()['Body'].read()
            if pq.ParquetFile(io.BytesIO(body)).metadata.num_rows == 0:
                empty.append(key)
        except Exception as e:
            log.warning(f"Could not inspect {key}, leaving it: {e}")
    return empty


with dag:
    # Dynamically generate tasks for each configured table
    ingestion_tasks = []
    for table in TABLES_CONFIG.keys():
        task = PythonOperator(
            task_id=f'ingest_{table}',
            python_callable=extract_and_load_table,
            op_kwargs={'table_name': table}
        )
        ingestion_tasks.append(task)

    # dbt transformations triggered after all ingestion finishes
    dbt_transformations = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/home/airflow/.local/bin/dbt"),
    )

    # After ingestion completes, branch off to execute both the analytical warehouse (dbt)
    # AND trigger the massive scalabiliy data lakehouse (Spark Iceberg)
    trigger_spark = TriggerDagRunOperator(
        task_id="trigger_spark_pipeline",
        trigger_dag_id="spark_transform_silver",
        wait_for_completion=False,
    )

    # Data-quality signals for Grafana / alerting
    data_quality_metrics = PythonOperator(
        task_id="emit_data_quality_metrics",
        python_callable=emit_data_quality_metrics,
    )

    # Drop Bronze partitions past their retention window. Runs after the
    # Spark trigger so the current extract is never removed before the
    # lakehouse has had the chance to read it.
    prune_bronze_task = PythonOperator(
        task_id="prune_bronze",
        python_callable=prune_bronze,
        # Runs even when an ingest task failed. This is cleanup, and gating it
        # on the work succeeding made a bad state self-perpetuating: an ingest
        # failure left prune_bronze upstream_failed, so the 0-row parquet files
        # it removes stayed in Bronze, so every silver transform kept failing on
        # them --
        #   Parquet column cannot be converted ... column: [order_id],
        #   physicalType: INT64, logicalType: int
        # -- and the lakehouse could not recover until someone cleaned the
        # bucket by hand. #144 sat in exactly that loop. Reclaiming space and
        # removing unreadable files are worth doing precisely when a run has
        # gone wrong.
        trigger_rule="all_done",
    )

    ingestion_tasks >> dbt_transformations
    ingestion_tasks >> trigger_spark
    ingestion_tasks >> data_quality_metrics
    trigger_spark >> prune_bronze_task
