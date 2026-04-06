"""
Spark Transformation DAG
Orchestrates PySpark jobs for Bronze -> Silver transformations on the data lake.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_transform_orders',
    default_args=default_args,
    description='Executes PySpark job mapping Bronze to Silver layer',
    schedule_interval='@daily',
    catchup=False,
    tags=['spark', 'transformation', 'silver'],
)

task_start = EmptyOperator(
    task_id='start_spark_pipeline',
    dag=dag,
)

spark_master   = os.environ.get('SPARK_MASTER_URL', 'spark://spark-master:7077')
minio_endpoint = os.environ.get('MINIO_ENDPOINT', 'http://minio:9000')
minio_user     = os.environ.get('MINIO_ROOT_USER', 'minioadmin')
minio_password = os.environ.get('MINIO_ROOT_PASSWORD', 'minioadmin')
executor_mem   = os.environ.get('SPARK_EXECUTOR_MEMORY', '1g')
executor_cores = os.environ.get('SPARK_EXECUTOR_CORES', '1')

spark_transform = SparkSubmitOperator(
    task_id='spark_transform_bronze_to_silver',
    application='/opt/spark-jobs/transform_orders.py',
    conn_id='spark_default',
    conf={
        'spark.master': spark_master,
        'spark.executor.memory': executor_mem,
        'spark.executor.cores': executor_cores,
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.hadoop.fs.s3a.endpoint': minio_endpoint,
        'spark.hadoop.fs.s3a.access.key': minio_user,
        'spark.hadoop.fs.s3a.secret.key': minio_password,
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    },
    application_args=['{{ ds_nodash }}'],
    dag=dag,
)

task_start >> spark_transform
