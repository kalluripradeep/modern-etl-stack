"""
Spark SQL Orchestration: Bronze -> Silver (Iceberg)
Parallelized transformation tasks for all source entities.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spark_transform_silver',
    default_args=default_args,
    description='Orchestrate PySpark jobs for Bronze to Silver Iceberg layer',
    schedule_interval='@daily',
    catchup=False,
    tags=['spark', 'iceberg', 'silver', 'multi-table'],
) as dag:

    # Helper function to generate standardized spark-submit commands
    def get_spark_submit_command(job_name, script_path):
        return f"""
    spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.executor.memory=1g \
        --conf spark.executor.cores=1 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4 \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.silver=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.silver.catalog-impl=org.apache.iceberg.hadoop.HadoopCatalog \
        --conf spark.sql.catalog.silver.warehouse=s3a://silver/ \
        --name {job_name} {script_path} {{{{ ds_nodash }}}}
    """

    transform_orders = BashOperator(
        task_id='transform_orders',
        bash_command=get_spark_submit_command('Orders-Bronze-to-Silver', '/opt/spark-jobs/transform_orders.py'),
    )

    transform_customers = BashOperator(
        task_id='transform_customers',
        bash_command=get_spark_submit_command('Customers-Bronze-to-Silver', '/opt/spark-jobs/transform_customers.py'),
    )

    transform_products = BashOperator(
        task_id='transform_products',
        bash_command=get_spark_submit_command('Products-Bronze-to-Silver', '/opt/spark-jobs/transform_products.py'),
    )

    transform_order_items = BashOperator(
        task_id='transform_order_items',
        bash_command=get_spark_submit_command('OrderItems-Bronze-to-Silver', '/opt/spark-jobs/transform_order_items.py'),
    )

    # Parallel execution
    [transform_orders, transform_customers, transform_products, transform_order_items]
