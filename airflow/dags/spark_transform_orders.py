"""
Spark Transformation DAG
Triggers Apache Spark job to transform Bronze → Silver
"""

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'pradeep',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 9),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_transform_orders',
    default_args=default_args,
    description='Transform orders using Apache Spark',
    schedule_interval='@daily',
    catchup=False,
    tags=['spark', 'transformation', 'silver'],
)


def print_start():
    print("Starting Spark transformation pipeline...")
    print(f"Date: {datetime.now().strftime('%Y%m%d')}")


task_start = PythonOperator(
    task_id='start_spark_pipeline',
    python_callable=print_start,
    dag=dag,
)

# All values configurable via env — defaults safe for local Docker, override for K8s
_spark_master   = os.environ.get('SPARK_MASTER_URL', 'spark://spark-master:7077')
_minio_endpoint = os.environ.get('MINIO_ENDPOINT', 'http://minio:9000')
_minio_user     = os.environ.get('MINIO_ROOT_USER', 'minioadmin')
_minio_password = os.environ.get('MINIO_ROOT_PASSWORD', 'minioadmin')
_executor_mem   = os.environ.get('SPARK_EXECUTOR_MEMORY', '1g')
_executor_cores = os.environ.get('SPARK_EXECUTOR_CORES', '1')

spark_transform = SparkSubmitOperator(
    task_id='spark_transform_bronze_to_silver',
    application='/opt/spark-jobs/transform_orders.py',
    conn_id='spark_default',
    conf={
        'spark.master': _spark_master,
        'spark.executor.memory': _executor_mem,
        'spark.executor.cores': _executor_cores,
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.hadoop.fs.s3a.endpoint': _minio_endpoint,
        'spark.hadoop.fs.s3a.access.key': _minio_user,
        'spark.hadoop.fs.s3a.secret.key': _minio_password,
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    },
    application_args=['{{ ds_nodash }}'],
    dag=dag,
)

task_start >> spark_transform
