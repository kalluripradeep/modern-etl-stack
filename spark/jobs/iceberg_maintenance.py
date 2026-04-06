"""
Spark Job: Iceberg Table Maintenance (Compaction & Z-Ordering)
Essential for 1 Billion+ record scalability to prevent the 'Small File Problem'.
"""

import os
import sys
from pyspark.sql import SparkSession


def create_spark_session():
    """Create Spark session with required Iceberg extensions for maintenance"""
    master_url = os.environ.get('SPARK_MASTER_URL', 'spark://spark-master:7077')
    return SparkSession.builder \
        .appName("Iceberg-Maintenance-Compaction-ZOrder") \
        .master(master_url) \
        .getOrCreate()


def run_maintenance(spark, table_name, z_order_col=None):
    """
    Performs maintenance tasks on an Iceberg table:
    1. rewriteDataFiles (Compaction)
    2. rewriteManifests
    3. rewriteDataFiles with Z-Ordering (if col provided)
    """
    print(f"\n--- Starting Maintenance for: {table_name} ---")
    
    # 1. Compaction: Merge small files into optimal chunks (default ~128MB)
    print(f"Running Compaction (rewrite_data_files) on {table_name}...")
    spark.sql(f"CALL silver.system.rewrite_data_files(table => '{table_name}')").show()
    
    # 2. Manifest Optimization: Speed up metadata lookups
    print(f"Running Manifest Optimization on {table_name}...")
    spark.sql(f"CALL silver.system.rewrite_manifests(table => '{table_name}')").show()
    
    # 3. Z-Ordering: Physically sort data by high-cardinality columns for instant filtering
    if z_order_col:
        print(f"Applying Z-Ordering on column '{z_order_col}' for {table_name}...")
        # Note: Iceberg Spark 3.3+ CALL procedure for Z-ordering
        spark.sql(f"""
            CALL silver.system.rewrite_data_files(
                table => '{table_name}',
                strategy => 'sort',
                sort_order => 'zorder({z_order_col})'
            )
        """).show()

    print(f"--- Maintenance Completed for: {table_name} ---\n")


def main():
    spark = create_spark_session()
    
    # Target our primary analytical tables
    tables_to_optimize = [
        ("silver.orders", "order_date"),
        ("silver.customers", "customer_id"),
        ("silver.order_items", "order_id"),
        ("silver.products", "category")
    ]
    
    try:
        for table, z_order_col in tables_to_optimize:
            if spark.catalog.tableExists(table):
                run_maintenance(spark, table, z_order_col)
            else:
                print(f"Table {table} does not exist yet. Skipping.")
                
        print("Global Iceberg Maintenance Completed Successfully!")

    except Exception as e:
        print(f"Maintenance Job Failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
