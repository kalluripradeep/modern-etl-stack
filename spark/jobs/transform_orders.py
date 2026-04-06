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
    """Create Spark session — relying on external --conf from SparkSubmit for flexibility"""
    master_url = os.environ.get('SPARK_MASTER_URL', 'spark://spark-master:7077')
    return SparkSession.builder \
        .appName("Orders-Bronze-to-Silver") \
        .master(master_url) \
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


def write_to_minio(df, date, spark):
    """Write Silver layer to MinIO using Apache Iceberg format"""
    print("Writing Silver layer Iceberg table...")
    catalog_name = "silver"
    table_name = f"{catalog_name}.orders"

    # Use Spark 3 writeTo API — it behaves much better with Iceberg catalogs
    print(f"Using writeTo API for {table_name}")
    df.writeTo(table_name) \
        .tableProperty("write.format.default", "parquet") \
        .partitionedBy("status") \
        .createOrReplace()

    print("Successfully wrote Silver layer Iceberg table")


def main():
    if len(sys.argv) < 2:
        date = "20260209"
        print(f"No date provided, using default: {date}")
    else:
        date = sys.argv[1]

    print(f"Starting Spark Job: Bronze to Silver — date: {date}")

    spark = create_spark_session()

    try:
        # Debug: Print configuration
        print("Spark Configurations:")
        for k, v in spark.sparkContext.getConf().getAll():
            if "catalog" in k or "s3a" in k:
                print(f"  {k}: {v}")

        bronze_df = read_from_minio(spark, date)
        # Show count to verify data is actually read
        print(f"Bronze record count: {bronze_df.count()}")
        bronze_df.show(5)

        silver_df = transform_to_silver(bronze_df)
        silver_df.show(5)

        write_to_minio(silver_df, date, spark)
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
