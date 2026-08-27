"""
Spark Job: Transform OrderItems from Bronze to Silver (Elite Scalability)
Uses Iceberg MERGE INTO for high-performance incremental transaction line-item updates.
"""

import os
import sys
from bronze import load_bronze
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


def create_spark_session():
    """Create Spark session — relying on external --conf for flexibility"""
    master_url = os.environ.get('SPARK_MASTER_URL', 'spark://spark-master:7077')
    return SparkSession.builder \
        .appName("OrderItems-Bronze-to-Silver-Incremental") \
        .master(master_url) \
        .getOrCreate()


def transform_order_items(df):
    """
    Applies Silver layer transformations:
    - Metadata tracking (processed_at)
    """
    print("Applying OrderItems Silver layer transformations...")
    return df.withColumn("created_at", col("created_at").cast("timestamp")) \
             .withColumn("updated_at", col("updated_at").cast("timestamp")) \
             .withColumn("processed_at", current_timestamp())


def upsert_to_iceberg(spark, df):
    """Upsert OrderItems to Silver Iceberg table"""
    catalog_name = "silver"
    table_name = f"{catalog_name}.lake.order_items"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.lake")

    # 1. Create table if not exists (first run)
    if not spark.catalog.tableExists(table_name):
        print(f"Creating new Iceberg table: {table_name}")
        df.writeTo(table_name) \
            .create()
        return

    # 2. Perform Incremental MERGE
    print(f"Performing MERGE INTO for {table_name}")
    df.createOrReplaceTempView("item_updates")

    # table_name is a constant defined in this file, not user input
    merge_sql = f"""
        MERGE INTO {table_name} AS target
        USING item_updates AS source
        ON target.item_id = source.item_id
        WHEN MATCHED THEN
            UPDATE SET 
                target.order_id = source.order_id,
                target.product_id = source.product_id,
                target.quantity = source.quantity,
                target.unit_price = source.unit_price,
                target.updated_at = source.updated_at,
                target.processed_at = source.processed_at
        WHEN NOT MATCHED THEN
            INSERT *
    """  # nosec B608
    spark.sql(merge_sql)
    print(f"Successfully merged order item updates into {table_name}")


def main():
    # No date argument: load_bronze reads every retained Bronze
    # partition, so a day this job missed is merged on the next run
    # rather than pruned away unmerged.
    spark = create_spark_session()
    
    try:
        print("Starting Spark Job: OrderItems Bronze to Silver")
        bronze_df = load_bronze(spark, "order_items", "silver.lake.order_items", "item_id")
        if bronze_df is None:
            return
        silver_df = transform_order_items(bronze_df)
        upsert_to_iceberg(spark, silver_df)
        print("Incremental OrderItems Spark Job Completed Successfully!")

    except Exception as e:
        print(f"Spark Job Failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
