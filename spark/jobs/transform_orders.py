"""
Spark Job: Transform Orders from Bronze to Silver
Simple version using CSV for demo
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date
from pyspark.sql.types import DecimalType
import sys

def create_spark_session():
    """Create Spark session"""
    return SparkSession.builder \
        .appName("Orders-Bronze-to-Silver") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

def read_from_local(spark, date):
    """Read Bronze layer from local file"""
    print(f"📦 Reading Bronze layer for date: {date}")
    
    # For demo, we'll read from the temp location
    df = spark.read.parquet(f"/opt/spark-data/orders_{date}.parquet")
    
    print(f"✅ Loaded {df.count()} records from Bronze")
    return df

def transform_to_silver(df):
    """Apply Silver layer transformations"""
    print("🔄 Applying Silver layer transformations...")
    
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
            "processed_at"
        )
    
    print(f"✅ Transformed to {silver_df.count()} valid records")
    return silver_df

def write_to_local(df, output_path):
    """Write Silver layer to local file"""
    print(f"💾 Writing to: {output_path}")
    
    df.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    print(f"✅ Successfully wrote Silver layer")

def main():
    """Main Spark job execution"""
    if len(sys.argv) < 2:
        date = "20260209"  # Default date
        print(f"⚠️ No date provided, using default: {date}")
    else:
        date = sys.argv[1]
    
    print("=" * 60)
    print("🚀 Starting Spark Job: Bronze → Silver Transformation")
    print(f"📅 Processing date: {date}")
    print("=" * 60)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Read Bronze data
        bronze_df = read_from_local(spark, date)
        
        # Show sample
        print("\n📊 Sample Bronze data:")
        bronze_df.show(5)
        
        # Transform to Silver
        silver_df = transform_to_silver(bronze_df)
        
        # Show sample
        print("\n📊 Sample Silver data:")
        silver_df.show(5)
        
        # Write to local
        output_path = f"/opt/spark-data/silver_orders_{date}.parquet"
        write_to_local(silver_df, output_path)
        
        print("=" * 60)
        print("✅ Spark Job Completed Successfully!")
        print(f"📍 Output: {output_path}")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Spark Job Failed: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()