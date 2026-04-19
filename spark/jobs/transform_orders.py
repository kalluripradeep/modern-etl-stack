"""
Spark Job: Transform Orders from Bronze to Silver (Elite Scalability)
Uses Iceberg MERGE INTO for high-performance incremental upserts.
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
        .appName("Orders-Bronze-to-Silver-Incremental") \
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
        .withColumn("created_at", col("created_at").cast("timestamp")) \
        .withColumn("updated_at", col("updated_at").cast("timestamp")) \
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


def upsert_to_iceberg(spark, df):
    """Upsert Silver layer to MinIO using Apache Iceberg MERGE INTO"""
    catalog_name = "silver"
    table_name = f"{catalog_name}.orders"

    # 1. Create table if not exists (first run)
    if not spark.catalog.tableExists(table_name):
        print(f"Creating new Iceberg table: {table_name}")
        df.writeTo(table_name) \
            .tableProperty("write.format.default", "parquet") \
            .partitionedBy("status") \
            .create()
        return

    # 2. Perform Incremental MERGE (Production Scalability)
    print(f"Performing MERGE INTO for {table_name}")
    df.createOrReplaceTempView("source_updates")

    merge_sql = f"""
        MERGE INTO {table_name} AS target
        USING source_updates AS source
        ON target.order_id = source.order_id
        WHEN MATCHED THEN
            UPDATE SET 
                target.total_amount = source.total_amount,
                target.status = source.status,
                target.updated_at = source.updated_at,
                target.processed_at = source.processed_at
        WHEN NOT MATCHED THEN
            INSERT *
    """
    spark.sql(merge_sql)
    print(f"Successfully merged updates into {table_name}")


def main():
    if len(sys.argv) < 2:
        date = datetime.now().strftime("%Y%m%d")
        print(f"No date provided, using today: {date}")
    else:
        date = sys.argv[1]

    spark = create_spark_session()

    try:
        bronze_df = read_from_minio(spark, date)
        silver_df = transform_to_silver(bronze_df)
        upsert_to_iceberg(spark, silver_df)
        print("Incremental Spark Job Completed Successfully!")

    except Exception as e:
        print(f"Spark Job Failed: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    from datetime import datetime
    main()
