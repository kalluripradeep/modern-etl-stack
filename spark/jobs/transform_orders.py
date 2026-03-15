"""
Spark Job: Transform Orders from Bronze to Silver
Reads from MinIO (S3A) bronze bucket, writes to MinIO silver bucket
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import DecimalType
import os
import sys


def create_spark_session():
    """Create Spark session — all config injected via env vars for K8s compatibility"""
    spark_master   = os.environ.get('SPARK_MASTER_URL', 'spark://spark-master:7077')
    minio_endpoint = os.environ.get('MINIO_ENDPOINT', 'http://minio:9000')
    minio_user     = os.environ.get('MINIO_ROOT_USER', 'minioadmin')
    minio_password = os.environ.get('MINIO_ROOT_PASSWORD', 'minioadmin')
    executor_mem   = os.environ.get('SPARK_EXECUTOR_MEMORY', '1g')

    return SparkSession.builder \
        .appName("Orders-Bronze-to-Silver") \
        .master(spark_master) \
        .config("spark.executor.memory", executor_mem) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_user) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_password) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
        .getOrCreate()


def read_from_minio(spark, date):
    """Read Bronze layer from MinIO — partitioned by date"""
    bronze_bucket = os.environ.get('BRONZE_BUCKET', 'bronze')
    # date format: YYYYMMDD → convert to YYYY/MM/DD partition path
    year, month, day = date[:4], date[4:6], date[6:]
    path = f"s3a://{bronze_bucket}/orders/{year}/{month}/{day}/"

    print(f"Reading Bronze layer from: {path}")
    df = spark.read.parquet(path)
    print(f"Loaded {df.count()} records from Bronze")
    return df


def transform_to_silver(df):
    """Apply Silver layer transformations"""
    print("Applying Silver layer transformations...")

    silver_df = df \
        .filter(col("order_id").isNotNull()) \
        .filter(col("customer_id").isNotNull()) \
        .filter(col("total_amount") > 0) \
        .filter(col("status").isNotNull()) \
        .withColumn("total_amount", col("total_amount").cast(DecimalType(10, 2))) \
        .withColumn("processed_at", current_timestamp()) \
        .select(
            "order_id",
            "customer_id",
            "order_date",
            "total_amount",
            "status",
            "created_at",
            "updated_at",
            "processed_at",
        )

    print(f"Transformed to {silver_df.count()} valid records")
    return silver_df


def write_to_minio(df, date):
    """Write Silver layer to MinIO — partitioned by date"""
    silver_bucket = os.environ.get('SILVER_BUCKET', 'silver')
    year, month, day = date[:4], date[4:6], date[6:]
    path = f"s3a://{silver_bucket}/orders/{year}/{month}/{day}/"

    print(f"Writing Silver layer to: {path}")
    df.write \
        .mode("overwrite") \
        .parquet(path)
    print(f"Successfully wrote Silver layer to {path}")


def main():
    if len(sys.argv) < 2:
        date = "20260209"
        print(f"No date provided, using default: {date}")
    else:
        date = sys.argv[1]

    print(f"Starting Spark Job: Bronze to Silver — date: {date}")

    spark = create_spark_session()

    try:
        bronze_df = read_from_minio(spark, date)
        bronze_df.show(5)

        silver_df = transform_to_silver(bronze_df)
        silver_df.show(5)

        write_to_minio(silver_df, date)
        print("Spark Job Completed Successfully!")

    except Exception as e:
        print(f"Spark Job Failed: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
