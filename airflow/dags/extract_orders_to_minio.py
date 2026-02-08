"""
Simple ETL Pipeline: PostgreSQL → MinIO
Extracts orders from PostgreSQL and saves as Parquet files in MinIO
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from io import BytesIO
from minio import Minio

# Default arguments for the DAG
default_args = {
    'owner': 'pradeep',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
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
        host='postgres-source',
        port=5432,
        database='sourcedb',
        user='sourceuser',
        password='sourcepass'
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

def load_to_minio():
    """Load Parquet file to MinIO"""
    print("📦 Connecting to MinIO...")
    
    client = Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin',
        secure=False
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
        content_type='application/octet-stream'
    )
    
    print(f"✅ Successfully uploaded to MinIO!")

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

# Define tasks
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
    task_id='load_to_minio',
    python_callable=load_to_minio,
    dag=dag,
)

# Define task dependencies
task_extract >> task_validate >> task_load