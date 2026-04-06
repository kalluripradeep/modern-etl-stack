"""
Spark Job: Transform Customers from Bronze to Silver (Elite Scalability)
Uses Iceberg MERGE INTO for high-performance incremental dimension updates.
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, trim, lower


def create_spark_session():
    """Create Spark session — relying on external --conf for flexibility"""
    master_url = os.environ.get('SPARK_MASTER_URL', 'spark://spark-master:7077')
    return SparkSession.builder \
        .appName("Customers-Bronze-to-Silver-Incremental") \
        .master(master_url) \
        .getOrCreate()


def read_from_bronze(spark, date_path):
    bronze_path = f"s3a://bronze/customers/{date_path}/"
    print(f"Reading Customer Bronze layer from: {bronze_path}")
    return spark.read.parquet(bronze_path)


def transform_customers(df):
    """
    Applies Silver layer transformations:
    - Data cleansing (trimming strings, lowering email)
    - Metadata tracking (processed_at)
    """
    print("Applying Customer Silver layer transformations...")
    return df.withColumn("first_name", trim(col("first_name"))) \
             .withColumn("last_name", trim(col("last_name"))) \
             .withColumn("email", lower(trim(col("email")))) \
             .withColumn("processed_at", current_timestamp())


def upsert_to_iceberg(spark, df):
    """Upsert Customers to Silver Iceberg table"""
    catalog_name = "silver"
    table_name = f"{catalog_name}.customers"

    # 1. Create table if not exists (first run)
    if not spark.catalog.tableExists(table_name):
        print(f"Creating new Iceberg table: {table_name}")
        df.writeTo(table_name) \
            .partitionedBy("state") \
            .create()
        return

    # 2. Perform Incremental MERGE (SCD Type 1 style)
    print(f"Performing MERGE INTO for {table_name}")
    df.createOrReplaceTempView("customer_updates")

    merge_sql = f"""
        MERGE INTO {table_name} AS target
        USING customer_updates AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN
            UPDATE SET 
                target.first_name = source.first_name,
                target.last_name = source.last_name,
                target.email = source.email,
                target.address = source.address,
                target.city = source.city,
                target.state = source.state,
                target.zip_code = source.zip_code,
                target.updated_at = source.updated_at,
                target.processed_at = source.processed_at
        WHEN NOT MATCHED THEN
            INSERT *
    """
    spark.sql(merge_sql)
    print(f"Successfully merged customer updates into {table_name}")


def main():
    if len(sys.argv) < 2:
        from datetime import datetime
        date_str = datetime.now().strftime("%Y%m%d")
    else:
        date_str = sys.argv[1]

    # Convert 20260405 to 2026/04/05 for S3 path
    formatted_date = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"

    spark = create_spark_session()
    
    try:
        print(f"Starting Spark Job: Customer Bronze to Silver — date: {date_str}")
        bronze_df = read_from_bronze(spark, formatted_date)
        silver_df = transform_customers(bronze_df)
        upsert_to_iceberg(spark, silver_df)
        print("Incremental Customer Spark Job Completed Successfully!")

    except Exception as e:
        print(f"Spark Job Failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
