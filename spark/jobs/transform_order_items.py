import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

def create_spark_session():
    """
    Creates a SparkSession with Iceberg and S3 configurations.
    Explicitly sets the master to ensure 'spark://' protocol is used.
    """
    return SparkSession.builder \
        .appName("OrderItems-Bronze-to-Silver") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.silver.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog") \
        .config("spark.sql.catalog.silver.warehouse", "s3a://silver/") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

def read_from_bronze(spark, date_path):
    bronze_path = f"s3a://bronze/order_items/{date_path}/"
    print(f"Reading OrderItems Bronze layer from: {bronze_path}")
    return spark.read.parquet(bronze_path)

def transform_order_items(df):
    """
    Applies Silver layer transformations:
    - Metadata tracking (processed_at)
    """
    print("Applying OrderItems Silver layer transformations...")
    return df.withColumn("processed_at", current_timestamp())

def write_to_minio(df, date, spark):
    catalog_name = "silver"
    table_name = f"{catalog_name}.order_items"

    # Use Spark 3 writeTo API
    # Partitioning by order_id might be too granular, let's keep it unpartitioned for small scale
    # or just use a hash partition if needed. For now, simple create.
    print(f"Using writeTo API for {table_name}")
    df.writeTo(table_name) \
        .createOrReplace()

def main():
    if len(sys.argv) < 2:
        print("Usage: transform_order_items.py <YYYYMMDD>")
        sys.exit(1)

    date_str = sys.argv[1]
    # Convert 20260405 to 2026/04/05
    formatted_date = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"

    spark = create_spark_session()
    
    try:
        print(f"Starting Spark Job: OrderItems Bronze to Silver — date: {date_str}")
        
        # 1. Read
        bronze_df = read_from_bronze(spark, formatted_date)
        print(f"Loaded {bronze_df.count()} order item records from Bronze")
        
        # 2. Transform
        silver_df = transform_order_items(bronze_df)
        
        # 3. Write
        write_to_minio(silver_df, date_str, spark)
        print(f"Successfully wrote {silver_df.count()} records to {formatted_date} Silver layer")

    except Exception as e:
        print(f"Spark Job Failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
