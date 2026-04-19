"""
Spark Job: Transform Products from Bronze to Silver (Elite Scalability)
Uses Iceberg MERGE INTO for high-performance incremental catalog updates.
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, trim, lower


def create_spark_session():
    """Create Spark session — relying on external --conf for flexibility"""
    master_url = os.environ.get('SPARK_MASTER_URL', 'spark://spark-master:7077')
    return SparkSession.builder \
        .appName("Products-Bronze-to-Silver-Incremental") \
        .master(master_url) \
        .getOrCreate()


def read_from_bronze(spark, date_path):
    bronze_path = f"s3a://bronze/products/{date_path}/"
    print(f"Reading Product Bronze layer from: {bronze_path}")
    return spark.read.parquet(bronze_path)


def transform_products(df):
    """
    Applies Silver layer transformations:
    - Data cleansing (trimming strings, lowering category)
    - Metadata tracking (processed_at)
    """
    print("Applying Product Silver layer transformations...")
    return df.withColumn("name", trim(col("name"))) \
             .withColumn("category", lower(trim(col("category")))) \
             .withColumn("created_at", col("created_at").cast("timestamp")) \
             .withColumn("updated_at", col("updated_at").cast("timestamp")) \
             .withColumn("processed_at", current_timestamp())


def upsert_to_iceberg(spark, df):
    """Upsert Products to Silver Iceberg table"""
    catalog_name = "silver"
    table_name = f"{catalog_name}.products"

    # 1. Create table if not exists (first run)
    if not spark.catalog.tableExists(table_name):
        print(f"Creating new Iceberg table: {table_name}")
        df.writeTo(table_name) \
            .partitionedBy("category") \
            .create()
        return

    # 2. Perform Incremental MERGE
    print(f"Performing MERGE INTO for {table_name}")
    df.createOrReplaceTempView("product_updates")

    merge_sql = f"""
        MERGE INTO {table_name} AS target
        USING product_updates AS source
        ON target.product_id = source.product_id
        WHEN MATCHED THEN
            UPDATE SET 
                target.name = source.name,
                target.description = source.description,
                target.price = source.price,
                target.category = source.category,
                target.stock_quantity = source.stock_quantity,
                target.updated_at = source.updated_at,
                target.processed_at = source.processed_at
        WHEN NOT MATCHED THEN
            INSERT *
    """
    spark.sql(merge_sql)
    print(f"Successfully merged product updates into {table_name}")


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
        print(f"Starting Spark Job: Product Bronze to Silver — date: {date_str}")
        bronze_df = read_from_bronze(spark, formatted_date)
        silver_df = transform_products(bronze_df)
        upsert_to_iceberg(spark, silver_df)
        print("Incremental Product Spark Job Completed Successfully!")

    except Exception as e:
        print(f"Spark Job Failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
