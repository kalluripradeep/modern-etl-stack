"""
Batch ETL Pipeline: PostgreSQL (Source) -> MinIO (Storage) -> PostgreSQL (Dest) -> dbt
Extracts batch order data, validates quality, uploads to object storage, and syncs downstream.
"""

import io
import os
import glob
import tempfile
import subprocess
import logging
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
from minio import Minio

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'extract_orders_to_minio',
    default_args=default_args,
    description='Extract batch orders from source and sync to data warehouse/lake',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'bronze', 'orders'],
)


def extract_and_load_batch():
    """
    Executes the extraction, validation, and load process atomically.
    Using a temporary directory ensures worker safety and avoids cross-task filesystem dependencies.
    """
    chunk_size = int(os.environ.get('ETL_CHUNK_SIZE', 10000))
    minio_user = os.environ.get('MINIO_ROOT_USER', 'minioadmin')
    minio_password = os.environ.get('MINIO_ROOT_PASSWORD', 'minioadmin')
    minio_endpoint = os.environ.get('MINIO_ENDPOINT', 'minio:9000').replace('http://', '').replace('https://', '')
    bucket_name = 'bronze'

    # Source Database Connection
    source_conn = psycopg2.connect(
        host=os.environ.get('SOURCE_DB_HOST', 'postgres-source'),
        port=int(os.environ.get('SOURCE_DB_PORT', 5432)),
        database=os.environ.get('SOURCE_DB_NAME', 'sourcedb'),
        user=os.environ.get('SOURCE_DB_USER', 'sourceuser'),
        password=os.environ.get('SOURCE_DB_PASSWORD', 'sourcepass'),
    )

    # Destination Database Connection
    dest_conn = psycopg2.connect(
        host=os.environ.get('DEST_DB_HOST', 'postgres-dest'),
        port=int(os.environ.get('DEST_DB_PORT', 5432)),
        database=os.environ.get('DEST_DB_NAME', 'destdb'),
        user=os.environ.get('DEST_DB_USER', 'destuser'),
        password=os.environ.get('DEST_DB_PASSWORD', 'destpass'),
    )
    
    # Minio Client Setup
    minio_client = Minio(minio_endpoint, access_key=minio_user, secret_key=minio_password, secure=False)
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    # Ensure target table exists in warehouse
    with dest_conn.cursor() as pg_cursor:
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
        dest_conn.commit()

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

    date_prefix = datetime.now().strftime("%Y/%m/%d")

    # Use a secure temp directory to store parquet partitions for this worker thread
    with tempfile.TemporaryDirectory() as tmpdirname:
        total_rows = 0
        
        # 1. Extract to Parquet Partitions
        log.info("Starting chunked extraction from source database.")
        for chunk_idx, df_chunk in enumerate(pd.read_sql(query, source_conn, chunksize=chunk_size)):
            out_path = os.path.join(tmpdirname, f'part-{chunk_idx:05d}.parquet')
            df_chunk.to_parquet(out_path, index=False, compression='snappy', coerce_timestamps='us', allow_truncated_timestamps=True)
            total_rows += len(df_chunk)
        
        log.info(f"Extracted {total_rows} total rows to {tmpdirname}")

        chunk_files = sorted(glob.glob(os.path.join(tmpdirname, 'part-*.parquet')))
        if not chunk_files:
            log.warning("No data extracted. Exiting.")
            return

        # 2. Validate, Upload to S3, and Load to DWH
        total_loaded = 0
        total_revenue = 0.0
        
        with dest_conn.cursor() as pg_cursor:
            for idx, file_path in enumerate(chunk_files):
                df_chunk = pd.read_parquet(file_path)
                
                # Validation checks
                if df_chunk['order_id'].isnull().any():
                    raise ValueError(f"Data quality error: Null order IDs detected in {file_path}")
                if (df_chunk['total_amount'] < 0).any():
                    raise ValueError(f"Data quality error: Negative amounts detected in {file_path}")
                
                total_revenue += float(df_chunk['total_amount'].sum())

                # Upload to MinIO Bronze layer
                object_name = f'orders/{date_prefix}/part-{idx:05d}.parquet'
                minio_client.fput_object(bucket_name, object_name, file_path)

                # UPSERT into Postgres via Temp Stage table for speed
                buffer = io.StringIO()
                df_chunk.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                pg_cursor.execute("CREATE TEMP TABLE IF NOT EXISTS orders_stage (LIKE public.orders) ON COMMIT PRESERVE ROWS")
                pg_cursor.execute("TRUNCATE orders_stage")
                pg_cursor.copy_expert(
                    "COPY orders_stage (order_id,customer_id,order_date,total_amount,status,created_at,updated_at) FROM STDIN WITH CSV",
                    buffer
                )
                
                pg_cursor.execute("""
                    INSERT INTO public.orders
                    SELECT * FROM orders_stage
                    ON CONFLICT (order_id) DO UPDATE SET
                        total_amount = EXCLUDED.total_amount,
                        status       = EXCLUDED.status,
                        updated_at   = EXCLUDED.updated_at
                """)
                dest_conn.commit()
                
                total_loaded += len(df_chunk)
                
        log.info(f"Successfully validated and loaded {total_loaded} rows. Total Revenue: ${total_revenue:,.2f}")

    source_conn.close()
    dest_conn.close()


def run_dbt_models():
    """Execute dbt models directly."""
    dbt_dir = '/opt/airflow/dbt'
    
    log.info(f"Running dbt project from {dbt_dir}")
    result = subprocess.run(
        ['dbt', 'run', '--profiles-dir', dbt_dir, '--project-dir', dbt_dir],
        capture_output=True,
        text=True,
    )
    
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError("dbt transformation failed during execution")


task_extract_load = PythonOperator(
    task_id='extract_and_load_batch',
    python_callable=extract_and_load_batch,
    dag=dag,
)

task_dbt = PythonOperator(
    task_id='run_dbt_transformations',
    python_callable=run_dbt_models,
    dag=dag,
)

task_extract_load >> task_dbt
