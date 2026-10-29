"""Spark half of the Spark/Trino Iceberg interop test.

Driven by scripts/test_lakehouse_interop.py, which runs this through the
Airflow image (the only place carrying the Iceberg and Postgres JARs) and
asserts on what it prints. Results go out as one JSON line behind a marker
rather than as a printed table, because spark-submit interleaves its own log
output with stdout and anything parsed positionally picks up log lines.

Each step is a separate submit: the point of the test is that two engines take
turns against one table, so the Spark session must genuinely end between them.
A single session holding the catalog open would prove nothing about what Trino
can see.
"""
import json
import sys

from pyspark.sql import SparkSession

# nosec B608 throughout: TABLE is the module constant directly below and
# nothing here takes outside input. Spark SQL cannot bind an identifier as
# a parameter, so the table name is interpolated by necessity.
TABLE = "silver.lake.interop_probe"
MARKER = "__INTEROP__"


def emit(spark, **extra):
    """Print the table's current shape, plus whatever the step wants to add."""
    rows = spark.sql(f"SELECT * FROM {TABLE} ORDER BY id").collect()  # nosec B608
    snaps = spark.sql(f"SELECT snapshot_id FROM {TABLE}.snapshots").collect()  # nosec B608
    payload = {
        "columns": [f.name for f in spark.table(TABLE).schema.fields],
        # str() everything: Decimal and date are not JSON-serialisable, and the
        # assertions compare against Trino's output, which is text anyway.
        "rows": [[None if v is None else str(v) for v in r] for r in rows],
        "snapshots": [str(r[0]) for r in snaps],
        **extra,
    }
    print(f"{MARKER} {json.dumps(payload)}")


def main():
    step = sys.argv[1]
    spark = SparkSession.builder.appName(f"interop-{step}").getOrCreate()
    # WARN, not ERROR. ERROR silences the one line that explains a submit
    # which hangs instead of failing:
    #
    #   WARN TaskSchedulerImpl: Initial job has not accepted any resources
    #
    # A master with no usable workers logs that every 15s and waits
    # forever. Under ERROR the run printed nothing at all until it timed
    # out -- ten minutes of silence on a cluster whose workers were gone.
    spark.sparkContext.setLogLevel("WARN")

    if step == "create":
        spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
        spark.sql(
            f"CREATE TABLE {TABLE} "
            "(id BIGINT, region STRING, amount DECIMAL(10,2)) USING iceberg"
        )
        spark.sql(
            f"INSERT INTO {TABLE} VALUES "  # nosec B608
            "(1,'emea',10.50),(2,'emea',20.25),(3,'apac',5.00)"
        )
        agg = spark.sql(
            f"SELECT region, count(*) n, sum(amount) total FROM {TABLE} "  # nosec B608
            "GROUP BY region ORDER BY region"
        ).collect()
        emit(spark, aggregate=[[str(v) for v in r] for r in agg])

    elif step == "evolve":
        spark.sql(f"ALTER TABLE {TABLE} ADD COLUMN channel STRING")
        spark.sql(f"INSERT INTO {TABLE} VALUES (4,'emea',7.75,'web')")  # nosec B608
        emit(spark)

    elif step == "readback":
        emit(spark)

    elif step == "drop":
        spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
        print(f"{MARKER} {json.dumps({'dropped': True})}")

    else:
        raise SystemExit(f"unknown step: {step}")

    spark.stop()


if __name__ == "__main__":
    main()
