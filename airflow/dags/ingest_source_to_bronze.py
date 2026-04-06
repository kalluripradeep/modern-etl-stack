"""
Batch ETL Pipeline: PostgreSQL (Source) -> MinIO (Storage) -> PostgreSQL (Dest) -> dbt
Generic ingestion for all source tables into the Bronze Lakehouse layer.
"""

import io
import os
import glob
import tempfile
import logging
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

log = logging.getLogger(__name__)

# --- Configuration for Source Tables ---
TABLES_CONFIG = {
    'orders': {
        'pk': 'order_id',
        'columns': ['order_id', 'customer_id', 'order_date', 'total_amount', 'status', 'created_at', 'updated_at'],
        'update_cols': ['total_amount', 'status', 'updated_at'],
        'ddl': """
            CREATE TABLE IF NOT EXISTS public.orders (
                order_id     BIGINT PRIMARY KEY,
                customer_id  BIGINT,
                order_date   TIMESTAMP,
                total_amount NUMERIC(18,2),
                status       TEXT,
                created_at   TIMESTAMP,
                updated_at   TIMESTAMP
            )
        """
    },
    'customers': {
        'pk': 'customer_id',
        'columns': ['customer_id', 'first_name', 'last_name', 'email', 'address', 'city', 'state', 'zip_code', 'created_at', 'updated_at'],
        'update_cols': ['first_name', 'last_name', 'email', 'address', 'city', 'state', 'zip_code', 'updated_at'],
        'ddl': """
            CREATE TABLE IF NOT EXISTS public.customers (
                customer_id  BIGINT PRIMARY KEY,
                first_name   TEXT,
                last_name    TEXT,
                email        TEXT UNIQUE,
                address      TEXT,
                city         TEXT,
                state        TEXT,
                zip_code     TEXT,
                created_at   TIMESTAMP,
                updated_at   TIMESTAMP
            )
        """
    },
    'products': {
        'pk': 'product_id',
        'columns': ['product_id', 'name', 'description', 'price', 'category', 'stock_quantity', 'created_at', 'updated_at'],
        'update_cols': ['name', 'description', 'price', 'category', 'stock_quantity', 'updated_at'],
        'ddl': """
            CREATE TABLE IF NOT EXISTS public.products (
                product_id     BIGINT PRIMARY KEY,
                name           TEXT,
                description    TEXT,
                price          NUMERIC(18,2),
                category       TEXT,
                stock_quantity INTEGER,
                created_at     TIMESTAMP,
                updated_at     TIMESTAMP
            )
        """
    },
    'order_items': {
        'pk': 'item_id',
        'columns': ['item_id', 'order_id', 'product_id', 'quantity', 'unit_price', 'created_at', 'updated_at'],
        'update_cols': ['quantity', 'unit_price', 'updated_at'],
        'ddl': """
            CREATE TABLE IF NOT EXISTS public.order_items (
                item_id    BIGINT PRIMARY KEY,
                order_id   BIGINT,
                product_id BIGINT,
                quantity   INTEGER,
                unit_price NUMERIC(18,2),
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        """
    }
}

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

DBT_PROJECT_PATH = Path("/opt/airflow/dbt")

profile_config = ProfileConfig(
    profile_name="modern_etl",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="dest_postgres",
        profile_args={"schema": "public"},
    ),
)

dag = DAG(
    'ingest_source_to_bronze',
    default_args=default_args,
    description='Extract all source tables from postgres to data lake (Bronze)',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'bronze', 'multi-table'],
)


def extract_and_load_table(table_name, **kwargs):
    """
    Generic extraction and loading function for any source table.
    """
    config = TABLES_CONFIG[table_name]
    logical_date = kwargs.get('logical_date', datetime.now())
    date_prefix = logical_date.strftime("%Y/%m/%d")
    
    chunk_size = int(os.environ.get('ETL_CHUNK_SIZE', 10000))
    bucket_name = 'bronze'

    source_hook = PostgresHook(postgres_conn_id='source_postgres')
    dest_hook = PostgresHook(postgres_conn_id='dest_postgres')
    s3_hook = S3Hook(aws_conn_id='minio_s3')

    if not s3_hook.check_for_bucket(bucket_name):
        s3_hook.create_bucket(bucket_name=bucket_name)

    # 1. Ensure target table exists in DWH
    dest_hook.run(config['ddl'])

    # 2. Extract from Source
    cols_str = ",".join(config['columns'])
    query = f"SELECT {cols_str} FROM {table_name} ORDER BY {config['pk']}"
    source_engine = source_hook.get_sqlalchemy_engine()

    with tempfile.TemporaryDirectory() as tmpdirname:
        total_rows = 0
        log.info(f"Starting extraction for table: {table_name}")
        
        for chunk_idx, df_chunk in enumerate(pd.read_sql(query, source_engine, chunksize=chunk_size)):
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
                    if df_chunk[config['pk']].isnull().any():
                        raise ValueError(f"Data quality error: Null PKs in {table_name} at {file_path}")

                    # Upload to MinIO
                    object_name = f'{table_name}/{date_prefix}/part-{idx:05d}.parquet'
                    s3_hook.load_file(filename=file_path, key=object_name, bucket_name=bucket_name, replace=True)

                    # Upsert into DWH
                    buffer = io.StringIO()
                    df_chunk.to_csv(buffer, index=False, header=False)
                    buffer.seek(0)

                    stage_table = f"{table_name}_stage"
                    pg_cursor.execute(f"CREATE TEMP TABLE IF NOT EXISTS {stage_table} (LIKE public.{table_name}) ON COMMIT PRESERVE ROWS")
                    pg_cursor.execute(f"TRUNCATE {stage_table}")
                    
                    pg_cursor.copy_expert(f"COPY {stage_table} ({cols_str}) FROM STDIN WITH CSV", buffer)
                    
                    update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in config['update_cols']])
                    upsert_sql = f"""
                        INSERT INTO public.{table_name}
                        SELECT * FROM {stage_table}
                        ON CONFLICT ({config['pk']}) DO UPDATE SET
                            {update_set}
                    """
                    pg_cursor.execute(upsert_sql)
                    dest_conn.commit()
                    total_loaded += len(df_chunk)
                
        log.info(f"Successfully synced {total_loaded} rows for {table_name}")


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
        execution_config=ExecutionConfig(dbt_executable_path="/usr/local/bin/dbt"),
    )

    ingestion_tasks >> dbt_transformations
