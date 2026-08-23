"""Shared Bronze reader for the Silver transform jobs.

Every one of the four transform jobs used to carry its own copy of this, and
every copy had the same two faults. #149's new lakehouse check found the
result: `iceberg.lake.orders` did not exist on a cluster whose
spark_transform_silver DAG had been green for days.

Fault one — success at doing nothing. Ingestion is incremental, so a run with
no new source rows writes no Bronze parquet at all. Each job then read the
missing path, caught AnalysisException, returned None, and main() returned. The
task exited 0. Nothing distinguished "no new rows to merge", which is a genuine
no-op, from "the table has never been created", which is a broken pipeline. A
cluster reported three consecutive successful runs at 3m27s while the lakehouse
did not exist.

Fault two — only today's partition was ever read. Bronze is pruned after 7
days (BRONZE_RETENTION_DAYS), and the jobs read exactly one day, so any day the
Spark side did not run was pruned before anything merged it and was gone from
the lake permanently. Nothing detected that either: the following day's run
read the following day's partition and reported success.

Both are fixed here rather than in four places.
"""

import os

from pyspark.errors import AnalysisException


def read_all_partitions(spark, source_table):
    """Read every retained Bronze partition for `source_table`.

    Reads the table prefix rather than one YYYY/MM/DD partition under it, so
    Spark discovers whatever is there and a day the job missed is merged on the
    next run instead of being lost when retention prunes it.

    The cost is re-merging up to BRONZE_RETENTION_DAYS of parquet on every run.
    MERGE is keyed on the primary key so that is idempotent, just not free: at
    a volume where it stops being cheap, track merged partitions and read only
    the unmerged ones, which needs state this pipeline does not currently keep.
    """
    bucket = os.environ.get('BRONZE_BUCKET', 'bronze')
    path = f"s3a://{bucket}/{source_table}_source/"
    print(f"Reading Bronze layer from: {path}")
    try:
        # recursiveFileLookup is required, not optional. The dated directories
        # under the prefix are plain YYYY/MM/DD, not Hive-style key=value, so
        # without it Spark treats them as partition directories, infers no
        # schema and raises AnalysisException on a prefix that plainly holds
        # parquet. Verified: the prefix read comes back empty without it.
        df = spark.read.option("recursiveFileLookup", "true").parquet(path)
    except AnalysisException:
        return None
    return df


def load_bronze(spark, source_table, iceberg_table):
    """Return the Bronze DataFrame, or None when there is genuinely nothing to do.

    Raises when there is no Bronze data *and* the target Iceberg table does not
    exist. That combination is not a quiet day, it is a lakehouse that was
    never initialised, and reporting it as success is what hid #149's failure
    for as long as it did.
    """
    df = read_all_partitions(spark, source_table)
    count = df.count() if df is not None else 0

    if count == 0:
        if not _table_exists(spark, iceberg_table):
            raise RuntimeError(
                f"No Bronze data for '{source_table}' and {iceberg_table} does "
                f"not exist, so the lakehouse has never been initialised. This "
                f"is a failure, not an empty run.\n"
                f"  Check that ingest_source_to_bronze has completed at least "
                f"once and wrote s3a://{os.environ.get('BRONZE_BUCKET', 'bronze')}"
                f"/{source_table}_source/, and that Bronze retention "
                f"(BRONZE_RETENTION_DAYS) has not pruned every partition."
            )
        print(f"No Bronze data for '{source_table}'; {iceberg_table} already "
              f"exists, so there is nothing to merge.")
        return None

    print(f"Loaded {count} records from Bronze")
    return df


def _table_exists(spark, iceberg_table):
    """tableExists, treating a catalog that cannot answer as "no".

    An Iceberg JDBC catalog creates its `iceberg_tables` relation lazily, on
    the first write. Until something has created a table, tableExists does not
    return False -- it raises:

        org.apache.iceberg.jdbc.UncheckedSQLException:
            Failed to get table lake.orders from catalog silver
        Caused by: PSQLException: relation "iceberg_tables" does not exist

    Which is the exact condition this function is asked about on a cluster
    where nothing has run yet, so letting it propagate turns "the lakehouse is
    uninitialised" -- a state with a clear message to print -- back into an
    opaque Py4J stack trace. An unanswerable catalog holds no tables, so the
    answer is no.

    Deliberately broad: the failures worth distinguishing here (bad JDBC
    credentials, unreachable Postgres) surface immediately afterwards on the
    write path with their own errors, and none of them are made worse by
    having concluded the table is absent.
    """
    try:
        return spark.catalog.tableExists(iceberg_table)
    except Exception as e:  # noqa: BLE001 - see above
        print(f"Could not query the catalog for {iceberg_table} "
              f"({type(e).__name__}); treating it as absent.")
        return False
