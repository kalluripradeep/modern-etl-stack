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
        'spark.jars.packages': 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4',
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.silver': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.silver.type': 'hadoop',
        'spark.sql.catalog.silver.warehouse': 's3a://silver/',
        'spark.sql.catalog.silver.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
    },
    application_args=['{{ ds_nodash }}'],
    dag=dag,
)

task_start >> spark_transform
