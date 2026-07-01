from pyspark.sql import SparkSession

def main():
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("Query-Lakehouse") \
        .getOrCreate()

    print("\n--- Querying Silver Iceberg Catalog ---")
    try:
        # Querying Iceberg Silver Table
        orders_df = spark.sql("SELECT * FROM silver.orders")
        record_count = orders_df.count()
        print(f"Total records in Iceberg 'silver.orders': {record_count}")
        
        print("\nTop 5 latest orders in the Data Lake:")
        spark.sql("SELECT order_id, customer_id, total_amount, status, updated_at FROM silver.orders ORDER BY updated_at DESC LIMIT 5").show(truncate=False)
        
    except Exception as e:
        print(f"Error querying Silver Iceberg table: {e}")
        print("Note: If the table doesn't exist, make sure the Spark Iceberg job has run in Airflow!")

    print("\n--- Querying Bronze Raw Parquet ---")
    try:
        # Querying raw Parquet files directly from MinIO
        bronze_df = spark.read.parquet("s3a://bronze/orders_source/*/*/*/")
        bronze_count = bronze_df.count()
        print(f"Total raw records in Bronze 'orders_source': {bronze_count}")
    except Exception as e:
        print(f"Error querying Bronze Parquet files: {e}")

    spark.stop()

if __name__ == "__main__":
    main()
