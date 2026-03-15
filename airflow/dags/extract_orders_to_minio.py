"""
Simple ETL Pipeline: PostgreSQL → MinIO → dbt
Extracts orders from PostgreSQL, saves to MinIO and PostgreSQL, then runs dbt transformations
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import glob as glob_module
import io
import os
import pandas as pd
import psycopg2
from minio import Minio

default_args = {
    'owner': 'pradeep',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'extract_orders_to_minio',
    default_args=default_args,
    description='Extract orders from PostgreSQL and save to MinIO',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'bronze', 'orders'],
)

# Configurable via env — small default, override for large datasets
CHUNK_SIZE = int(os.environ.get('ETL_CHUNK_SIZE', 10_000))
CHUNK_DIR = '/tmp/orders_chunks'


def extract_from_postgres():
    """Extract orders from PostgreSQL source database in chunks — streams data, never loads full table into RAM"""

    conn = psycopg2.connect(
        host=os.environ.get('SOURCE_DB_HOST', 'postgres-source'),
        port=int(os.environ.get('SOURCE_DB_PORT', 5432)),
        database=os.environ.get('SOURCE_DB_NAME', 'sourcedb'),
        user=os.environ.get('SOURCE_DB_USER', 'sourceuser'),
        password=os.environ.get('SOURCE_DB_PASSWORD', 'sourcepass'),
    )

    query = """
        SELECT
            order_id,
            customer_id,
            order_date,
            total_amount,
            status,
            created_at,
            updated_at
        FROM orders
        ORDER BY order_id
    """

    os.makedirs(CHUNK_DIR, exist_ok=True)

    total_rows = 0
    chunk_num = 0

    # chunksize= streams CHUNK_SIZE rows at a time — never loads full table into RAM
    for chunk in pd.read_sql(query, conn, chunksize=CHUNK_SIZE):
        out_path = f'{CHUNK_DIR}/part-{chunk_num:05d}.parquet'
        chunk.to_parquet(out_path, index=False, compression='snappy', coerce_timestamps='us', allow_truncated_timestamps=True)
        total_rows += len(chunk)
        chunk_num += 1
        print(f"Chunk {chunk_num}: {total_rows:,} rows written")

    conn.close()
    print(f"Extracted {total_rows:,} rows across {chunk_num} files")
    return total_rows


def validate_data():
    """Validate data quality chunk by chunk — never loads full dataset into RAM"""

    chunk_files = sorted(glob_module.glob(f'{CHUNK_DIR}/part-*.parquet'))
    assert len(chunk_files) > 0, "No chunk files found!"

    total_rows = 0
    total_revenue = 0.0

    for f in chunk_files:
        chunk = pd.read_parquet(f)
        assert chunk['order_id'].notna().all(), f"Null order IDs in {f}"
        assert (chunk['total_amount'] >= 0).all(), f"Negative amounts in {f}"
        total_rows += len(chunk)
        total_revenue += chunk['total_amount'].sum()
        del chunk  # release memory after each chunk

    print(f"Validation passed: {total_rows:,} rows, ${total_revenue:,.2f} revenue")


def load_to_minio_and_postgres():
    """Load chunk files to MinIO (partitioned) and PostgreSQL (via COPY — fastest bulk load)"""

    minio_user = os.environ.get('MINIO_ROOT_USER', 'minioadmin')
    minio_password = os.environ.get('MINIO_ROOT_PASSWORD', 'minioadmin')
    client = Minio('minio:9000', access_key=minio_user, secret_key=minio_password, secure=False)

    bucket_name = 'bronze'
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    dest_user = os.environ.get('DEST_DB_USER', 'destuser')
    dest_password = os.environ.get('DEST_DB_PASSWORD', 'destpass')
    dest_db = os.environ.get('DEST_DB_NAME', 'destdb')
    dest_host = os.environ.get('DEST_DB_HOST', 'postgres-dest')

    # Use raw psycopg2 for COPY — 50x faster than to_sql
    pg_conn = psycopg2.connect(
        host=dest_host, port=5432,
        database=dest_db, user=dest_user, password=dest_password,
    )
    pg_cursor = pg_conn.cursor()

    # Create table once — never DROP TABLE, use upsert to stay incremental
    pg_cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.orders (
            order_id     BIGINT PRIMARY KEY,
            customer_id  BIGINT,
            order_date   TIMESTAMP,
            total_amount NUMERIC(18,2),
            status       TEXT,
            created_at   TIMESTAMP,
            updated_at   TIMESTAMP
        )
    """)
    pg_conn.commit()

    date_prefix = datetime.now().strftime("%Y/%m/%d")

    # Delete all existing objects at this date prefix — ensures stale files don't linger
    prefix = f'orders/{date_prefix}/'
    existing_objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
    for obj in existing_objects:
        client.remove_object(bucket_name, obj.object_name)
        print(f"Deleted stale object: {obj.object_name}")

    chunk_files = sorted(glob_module.glob(f'{CHUNK_DIR}/part-*.parquet'))
    total_loaded = 0

    for i, file_path in enumerate(chunk_files):
        chunk = pd.read_parquet(file_path)

        # Upload each chunk as its own MinIO object — enables parallel reads later
        object_name = f'orders/{date_prefix}/part-{i:05d}.parquet'
        client.fput_object(bucket_name, object_name, file_path)

        # COPY via staging table — fastest possible Postgres bulk load
        buffer = io.StringIO()
        chunk.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        pg_cursor.execute("""
            CREATE TEMP TABLE IF NOT EXISTS orders_stage
            (LIKE public.orders) ON COMMIT PRESERVE ROWS
        """)
        pg_cursor.execute("TRUNCATE orders_stage")
        pg_cursor.copy_expert(
            "COPY orders_stage (order_id,customer_id,order_date,total_amount,status,created_at,updated_at) FROM STDIN WITH CSV",
            buffer,
        )
        pg_cursor.execute("""
            INSERT INTO public.orders
            SELECT * FROM orders_stage
            ON CONFLICT (order_id) DO UPDATE SET
                total_amount = EXCLUDED.total_amount,
                status       = EXCLUDED.status,
                updated_at   = EXCLUDED.updated_at
        """)
        pg_conn.commit()

        total_loaded += len(chunk)
        del chunk
        print(f"Chunk {i+1}/{len(chunk_files)}: {total_loaded:,} rows loaded")

    pg_cursor.close()
    pg_conn.close()
    print(f"Done: {total_loaded:,} rows → MinIO s3://{bucket_name}/orders/{date_prefix}/ + PostgreSQL {dest_db}.public.orders")


def run_dbt_models():
    """Run dbt transformations"""
    import subprocess

    dbt_dir = '/opt/airflow/dbt'

    result = subprocess.run(
        ['dbt', 'run', '--profiles-dir', dbt_dir, '--project-dir', dbt_dir],
        capture_output=True,
        text=True,
    )

    print(result.stdout)

    if result.returncode == 0:
        print("dbt models ran successfully!")
    else:
        print("dbt run failed!")
        print(result.stderr)
        raise Exception("dbt run failed")


task_extract = PythonOperator(
    task_id='extract_from_postgres',
    python_callable=extract_from_postgres,
    dag=dag,
)

task_validate = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

task_load = PythonOperator(
    task_id='load_to_minio_and_postgres',
    python_callable=load_to_minio_and_postgres,
    dag=dag,
)

task_dbt = PythonOperator(
    task_id='run_dbt_transformations',
    python_callable=run_dbt_models,
    dag=dag,
)

task_extract >> task_validate >> task_load >> task_dbt
