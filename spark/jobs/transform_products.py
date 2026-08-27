"""
Spark Job: Transform Products from Bronze to Silver (Elite Scalability)
Uses Iceberg MERGE INTO for high-performance incremental catalog updates.
"""

import os
import sys
from bronze import load_bronze
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, trim, lower


def create_spark_session():
    """Create Spark session — relying on external --conf for flexibility"""
    master_url = os.environ.get('SPARK_MASTER_URL', 'spark://spark-master:7077')
    return SparkSession.builder \
        .appName("Products-Bronze-to-Silver-Incremental") \
        .master(master_url) \
        .getOrCreate()


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
    table_name = f"{catalog_name}.lake.products"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.lake")

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

    # table_name is a constant defined in this file, not user input
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
    """  # nosec B608
    spark.sql(merge_sql)
    print(f"Successfully merged product updates into {table_name}")


def main():
    # No date argument: load_bronze reads every retained Bronze
    # partition, so a day this job missed is merged on the next run
    # rather than pruned away unmerged.
    spark = create_spark_session()
    
    try:
        print("Starting Spark Job: Product Bronze to Silver")
        bronze_df = load_bronze(spark, "products", "silver.lake.products", "product_id")
        if bronze_df is None:
            return
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
