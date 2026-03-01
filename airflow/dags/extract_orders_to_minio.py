"""
Simple ETL Pipeline: PostgreSQL → MinIO → dbt
Extracts orders from PostgreSQL, saves to MinIO and PostgreSQL, then runs dbt transformations
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import psycopg2
from io import BytesIO
from minio import Minio
from sqlalchemy import create_engine, text

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

def extract_from_postgres():
    """Extract orders from PostgreSQL source database"""
    print("📊 Connecting to PostgreSQL...")

    conn = psycopg2.connect(
        host=os.environ.get('SOURCE_DB_HOST', 'postgres-source'),
        port=int(os.environ.get('SOURCE_DB_PORT', 5432)),
        database=os.environ.get('SOURCE_DB_NAME', 'sourcedb'),
        user=os.environ.get('SOURCE_DB_USER', 'sourceuser'),
        password=os.environ.get('SOURCE_DB_PASSWORD', 'sourcepass'),
    )

    print("📦 Extracting orders...")
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

    df = pd.read_sql(query, conn)
    conn.close()

    print(f"✅ Extracted {len(df)} orders")
    df.to_parquet('/tmp/orders.parquet', index=False)
    return len(df)

def validate_data():
    """Simple data quality check"""
    print("🔍 Validating data...")

    df = pd.read_parquet('/tmp/orders.parquet')

    assert len(df) > 0, "No data extracted!"
    assert df['order_id'].notna().all(), "Found null order IDs!"
    assert (df['total_amount'] >= 0).all(), "Found negative amounts!"

    print(f"✅ Data quality checks passed!")
    print(f"   - Total orders: {len(df)}")
    print(f"   - Total revenue: ${df['total_amount'].sum():.2f}")

def load_to_minio_and_postgres():
    """Load Parquet file to MinIO AND PostgreSQL destination"""

    print("📦 Connecting to MinIO...")

    minio_user = os.environ.get('MINIO_ROOT_USER', 'minioadmin')
    minio_password = os.environ.get('MINIO_ROOT_PASSWORD', 'minioadmin')

    client = Minio(
        'minio:9000',
        access_key=minio_user,
        secret_key=minio_password,
        secure=False,
    )

    bucket_name = 'bronze'
    if not client.bucket_exists(bucket_name):
        print(f"🪣 Creating bucket: {bucket_name}")
        client.make_bucket(bucket_name)

    file_path = '/tmp/orders.parquet'
    object_name = f'orders/orders_{datetime.now().strftime("%Y%m%d")}.parquet'

    print(f"⬆️  Uploading to s3://{bucket_name}/{object_name}")
    client.fput_object(
        bucket_name,
        object_name,
        file_path,
        content_type='application/octet-stream',
    )
    print(f"✅ Uploaded to MinIO!")

    print("📊 Loading data into PostgreSQL destination...")
    df = pd.read_parquet(file_path)

    dest_user = os.environ.get('DEST_DB_USER', 'destuser')
    dest_password = os.environ.get('DEST_DB_PASSWORD', 'destpass')
    dest_db = os.environ.get('DEST_DB_NAME', 'destdb')
    engine = create_engine(f'postgresql://{dest_user}:{dest_password}@postgres-dest:5432/{dest_db}')

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS public"))
        conn.execute(text("DROP TABLE IF EXISTS public.orders CASCADE"))

    df.to_sql(
        'orders',
        engine,
        schema='public',
        if_exists='replace',
        index=False,
        method='multi',
    )

    print(f"✅ Loaded {len(df)} rows into PostgreSQL!")
    print(f"📍 MinIO: s3://{bucket_name}/{object_name}")
    print(f"📍 PostgreSQL: {dest_db}.public.orders")

def run_dbt_models():
    """Run dbt transformations"""
    import subprocess

    print("🔄 Running dbt transformations...")

    dbt_dir = '/opt/airflow/dbt'

    result = subprocess.run(
        ['dbt', 'run', '--profiles-dir', dbt_dir, '--project-dir', dbt_dir],
        capture_output=True,
        text=True,
    )

    print(result.stdout)

    if result.returncode == 0:
        print("✅ dbt models ran successfully!")
    else:
        print("❌ dbt run failed!")
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
