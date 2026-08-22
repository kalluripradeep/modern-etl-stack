#!/usr/bin/env python3
"""Guard the Bronze load contract in spark/jobs/bronze.py.

The bug this protects against did not look like a bug. Four Spark jobs read a
Bronze partition that did not exist, returned None, and exited 0, so
spark_transform_silver was green for days on a cluster where
iceberg.lake.orders had never been created (#149).

The distinction that matters is between "no new rows to merge", which is a
legitimate empty run, and "no Bronze data and no target table", which means
the lakehouse was never initialised. The first must return None, the second
must raise. That is the whole contract, and it gets a test rather than a
comment.

Fakes stand in for Spark: none of these paths call anything but count() and
catalog.tableExists(), so a session is not needed and this runs in CI.
"""

import sys
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "spark" / "jobs"))


class FakeCatalog:
    def __init__(self, exists):
        self._exists = exists
        self.asked = []

    def tableExists(self, name):   # noqa: N802 - mirrors the PySpark API
        self.asked.append(name)
        return self._exists


class FakeSpark:
    def __init__(self, table_exists):
        self.catalog = FakeCatalog(table_exists)


class FakeDF:
    def __init__(self, rows):
        self._rows = rows

    def count(self):
        return self._rows


def main():
    import bronze

    # 1. No Bronze data and no table: the lakehouse was never initialised.
    #    This is the case that used to exit 0 and take Pipe 2 down silently.
    with mock.patch.object(bronze, "read_all_partitions", return_value=None):
        try:
            bronze.load_bronze(FakeSpark(table_exists=False), "orders", "silver.lake.orders")
        except RuntimeError as e:
            assert "never been initialised" in str(e), str(e)
            assert "silver.lake.orders" in str(e), str(e)
            assert "ingest_source_to_bronze" in str(e), "error should say what to check"
        else:
            raise AssertionError("no data and no table must raise, not return")

    # 2. An empty frame is the same condition as no frame at all.
    with mock.patch.object(bronze, "read_all_partitions", return_value=FakeDF(0)):
        try:
            bronze.load_bronze(FakeSpark(table_exists=False), "orders", "silver.lake.orders")
        except RuntimeError:
            pass
        else:
            raise AssertionError("an empty frame with no table must raise too")

    # 3. No Bronze data but the table exists: a genuinely quiet run.
    with mock.patch.object(bronze, "read_all_partitions", return_value=None):
        assert bronze.load_bronze(
            FakeSpark(table_exists=True), "orders", "silver.lake.orders"
        ) is None

    # 4. Rows present: hand them back, and do not consult the catalog at all.
    df = FakeDF(42)
    spark = FakeSpark(table_exists=False)
    with mock.patch.object(bronze, "read_all_partitions", return_value=df):
        assert bronze.load_bronze(spark, "orders", "silver.lake.orders") is df
    assert spark.catalog.asked == [], "the happy path should not need tableExists"

    # 5. The reader must target the table prefix, not one dated partition.
    #    Reading a single day is what let retention prune unmerged partitions.
    spark = mock.MagicMock()
    bronze.read_all_partitions(spark, "orders")
    assert spark.read.option.call_args[0] == ("recursiveFileLookup", "true"), (
        "recursiveFileLookup is load-bearing: the dated directories are not "
        "Hive-style key=value, so without it Spark infers no schema and the "
        "prefix read silently comes back empty. Verified against MinIO."
    )
    path = spark.read.option.return_value.parquet.call_args[0][0]
    assert path == "s3a://bronze/orders_source/", path

    print("bronze load contract: OK")


if __name__ == "__main__":
    main()
