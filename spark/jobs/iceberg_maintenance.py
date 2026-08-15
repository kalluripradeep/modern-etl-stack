"""
Spark Job: Iceberg Table Maintenance (Compaction & Z-Ordering)
Essential for 1 Billion+ record scalability to prevent the 'Small File Problem'.
"""

import os
import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession


def create_spark_session():
    """Create Spark session with required Iceberg extensions for maintenance"""
    master_url = os.environ.get('SPARK_MASTER_URL', 'spark://spark-master:7077')
    return SparkSession.builder \
        .appName("Iceberg-Maintenance-Compaction-ZOrder") \
        .master(master_url) \
        .getOrCreate()


def run_maintenance(spark, table_name, z_order_col=None, compact=True):
    """
    Performs maintenance tasks on an Iceberg table.

    Reclaiming storage (expire_snapshots, remove_orphan_files) is cheap and
    runs every time. Reorganising it (compaction, manifest rewrite,
    Z-ordering) is expensive and only runs when `compact` is set -- weekly is
    plenty for layout, but waiting a week to release disk is what let a
    cluster fill up.
    """
    print(f"\n--- Starting Maintenance for: {table_name} "
          f"({'full' if compact else 'expire-only'}) ---")

    if compact:
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

    # 4. Expire old snapshots — this is the step that actually frees storage.
    #
    # Everything above REWRITES data: compaction and Z-ordering write new
    # files while the previous snapshots still reference the old ones. Iceberg
    # keeps every snapshot until it is expired, so without this the table only
    # ever grows, and running maintenance makes it grow FASTER rather than
    # smaller. A cluster running this pipeline hourly filled its node disk
    # and started evicting pods.
    #
    # Seven days keeps time-travel useful while bounding what is retained.
    print(f"Expiring snapshots older than 7 days on {table_name}...")
    spark.sql(f"""
        CALL silver.system.expire_snapshots(
            table => '{table_name}',
            older_than => TIMESTAMP '{
                (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
            }',
            retain_last => 5
        )
    """).show()

    # 5. Delete files no snapshot references any more. expire_snapshots drops
    # the metadata pointers; orphans are what compaction left behind when a
    # write failed partway. Iceberg refuses to look at anything younger than
    # three days by default, which protects in-flight writes.
    print(f"Removing orphan files on {table_name}...")
    spark.sql(f"CALL silver.system.remove_orphan_files(table => '{table_name}')").show()

    print(f"--- Maintenance Completed for: {table_name} ---\n")


def main():
    # Compaction reorganises data; expiry releases it. Doing both weekly
    # means up to seven days of snapshots accumulate before anything is
    # freed, which is too slow for an hourly pipeline. Expire every run,
    # compact on Sundays.
    compact = datetime.utcnow().isoweekday() == 7
    print(f"Maintenance mode: {'full (compact + expire)' if compact else 'expire-only'}")

    spark = create_spark_session()

    # Target our primary analytical tables
    tables_to_optimize = [
        ("silver.lake.orders", "order_date"),
        ("silver.lake.customers", "customer_id"),
        ("silver.lake.order_items", "order_id"),
        ("silver.lake.products", "product_id")
    ]
    
    try:
        for table, z_order_col in tables_to_optimize:
            if spark.catalog.tableExists(table):
                run_maintenance(spark, table, z_order_col, compact=compact)
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
