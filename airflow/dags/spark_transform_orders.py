"""
Spark Transformation DAG
Triggers Apache Spark job to transform Bronze → Silver
"""

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

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
    print("🚀 Starting Spark transformation pipeline...")
    print(f"📅 Date: {datetime.now().strftime('%Y%m%d')}")

task_start = PythonOperator(
    task_id='start_spark_pipeline',
    python_callable=print_start,
    dag=dag,
)

# Spark transformation task
spark_transform = SparkSubmitOperator(
    task_id='spark_transform_bronze_to_silver',
    application='/opt/spark-jobs/transform_orders.py',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.executor.memory': '1g',
        'spark.executor.cores': '1',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minioadmin',
        'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    },
    application_args=['{{ ds_nodash }}'],  # Pass date as argument
    dag=dag,
)

task_start >> spark_transform