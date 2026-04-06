"""
Batch ETL Pipeline: PostgreSQL (Source) -> MinIO (Storage) -> PostgreSQL (Dest) -> dbt
Extracts batch order data, validates quality, uploads to object storage, and syncs downstream.
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
    'extract_orders_to_minio',
    default_args=default_args,
    description='Extract batch orders from source and sync to data warehouse/lake',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'bronze', 'orders'],
)


def extract_and_load_batch(**kwargs):
    """
    Executes the extraction, validation, and load process atomically using Airflow Hooks.
    Uses logical_date from context for idempotent storage paths.
    """
    logical_date = kwargs.get('logical_date')
    if not logical_date:
        # Fallback for manual testing outside of Airflow context if needed
        logical_date = datetime.now()
        
    date_prefix = logical_date.strftime("%Y/%m/%d")
    chunk_size = int(os.environ.get('ETL_CHUNK_SIZE', 10000))
    bucket_name = 'bronze'

    # Native Airflow Hooks (No hardcoded os.environ credentials!)
    source_hook = PostgresHook(postgres_conn_id='source_postgres')
    dest_hook = PostgresHook(postgres_conn_id='dest_postgres')
    s3_hook = S3Hook(aws_conn_id='minio_s3')

    if not s3_hook.check_for_bucket(bucket_name):
        s3_hook.create_bucket(bucket_name=bucket_name)

    # Ensure target table exists in warehouse
    dest_hook.run("""
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

    source_engine = source_hook.get_sqlalchemy_engine()

    with tempfile.TemporaryDirectory() as tmpdirname:
        total_rows = 0
        
        # 1. Extract to Parquet Partitions
        log.info("Starting chunked extraction from source database.")
        for chunk_idx, df_chunk in enumerate(pd.read_sql(query, source_engine, chunksize=chunk_size)):
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
        
        with dest_hook.get_conn() as dest_conn:
            with dest_conn.cursor() as pg_cursor:
                for idx, file_path in enumerate(chunk_files):
                    df_chunk = pd.read_parquet(file_path)
                    
                    if df_chunk['order_id'].isnull().any():
                        raise ValueError(f"Data quality error: Null order IDs detected in {file_path}")
                    if (df_chunk['total_amount'] < 0).any():
                        raise ValueError(f"Data quality error: Negative amounts detected in {file_path}")
                    
                    total_revenue += float(df_chunk['total_amount'].sum())

                    # Upload to MinIO using S3Hook
                    object_name = f'orders/{date_prefix}/part-{idx:05d}.parquet'
                    s3_hook.load_file(filename=file_path, key=object_name, bucket_name=bucket_name, replace=True)

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


with dag:
    task_extract_load = PythonOperator(
        task_id='extract_and_load_batch',
        python_callable=extract_and_load_batch,
    )

    # Astronomer Cosmos DbtTaskGroup automatically parses and builds Airflow Tasks!
    dbt_transformations = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/usr/local/bin/dbt"),
    )

    task_extract_load >> dbt_transformations
