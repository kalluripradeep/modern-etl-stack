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

import io
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
        if isinstance(self._exists, Exception):
            raise self._exists
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
            bronze.load_bronze(FakeSpark(table_exists=False), "orders", "silver.lake.orders", "order_id")
        except RuntimeError as e:
            assert "never been initialised" in str(e), str(e)
            assert "silver.lake.orders" in str(e), str(e)
            assert "ingest_source_to_bronze" in str(e), "error should say what to check"
        else:
            raise AssertionError("no data and no table must raise, not return")

    # 2. An empty frame is the same condition as no frame at all.
    with (mock.patch.object(bronze, "read_all_partitions", return_value=FakeDF(0)),
          mock.patch.object(bronze, "_latest_per_key", side_effect=lambda d, k: d)):
        try:
            bronze.load_bronze(FakeSpark(table_exists=False), "orders", "silver.lake.orders", "order_id")
        except RuntimeError:
            pass
        else:
            raise AssertionError("an empty frame with no table must raise too")

    # 3. No Bronze data but the table exists: a genuinely quiet run.
    with mock.patch.object(bronze, "read_all_partitions", return_value=None):
        assert bronze.load_bronze(
            FakeSpark(table_exists=True), "orders", "silver.lake.orders", "order_id"
        ) is None

    # 4. Rows present: hand them back, and do not consult the catalog at all.
    df = FakeDF(42)
    spark = FakeSpark(table_exists=False)
    with (mock.patch.object(bronze, "read_all_partitions", return_value=df),
          mock.patch.object(bronze, "_latest_per_key", side_effect=lambda d, k: d)):
        assert bronze.load_bronze(spark, "orders", "silver.lake.orders", "order_id") is df
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

    # 6. A catalog that cannot answer must read as "no table", not blow up.
    #    An Iceberg JDBC catalog creates iceberg_tables lazily on first write,
    #    so on a cluster where nothing has run yet tableExists raises
    #    UncheckedSQLException instead of returning False. Letting that
    #    propagate turned "the lakehouse is uninitialised" back into a Py4J
    #    stack trace. Reproduced against a real fresh catalog before fixing.
    boom = RuntimeError("UncheckedSQLException: relation iceberg_tables does not exist")
    with mock.patch.object(bronze, "read_all_partitions", return_value=None):
        try:
            bronze.load_bronze(FakeSpark(table_exists=boom), "orders", "silver.lake.orders", "order_id")
        except RuntimeError as e:
            assert "never been initialised" in str(e), (
                "a catalog that raises should still produce the actionable "
                f"message, got: {e}"
            )
        else:
            raise AssertionError("a raising catalog must still raise on no data")

    # 7. Reading every partition means a key recurs once per run that touched
    #    it, and MERGE INTO rejects that with MERGE_CARDINALITY_VIOLATION.
    #    Deduping to the newest row per key is what makes the prefix read
    #    usable at all, so it is asserted rather than assumed.
    calls = {}

    class FakeCol:
        def __init__(self, name):
            self.name = name

        def desc(self):
            calls["ordered_desc_on"] = self.name
            return self

        def __eq__(self, other):
            return ("eq", self.name, other)

    class DedupeDF:
        def __init__(self):
            self.dropped = None

        def withColumn(self, *_):
            return self

        def filter(self, cond):
            calls["filtered"] = cond
            return self

        def drop(self, name):
            self.dropped = name
            return self

    fake_window = mock.MagicMock()
    fake_window.partitionBy.return_value.orderBy.return_value = "w"
    with mock.patch.dict(sys.modules, {
        "pyspark.sql": mock.MagicMock(Window=fake_window),
        "pyspark.sql.functions": mock.MagicMock(
            col=FakeCol, row_number=mock.MagicMock()),
    }):
        out = bronze._latest_per_key(DedupeDF(), "order_id")

    assert fake_window.partitionBy.call_args[0][0] == "order_id", (
        "must partition by the primary key")
    assert calls.get("ordered_desc_on") == "updated_at", (
        "must keep the newest row, ordering by updated_at descending, "
        f"got {calls.get('ordered_desc_on')!r}")
    assert out.dropped == "_row", "the helper column must not reach the MERGE"

    # 8. An incompatible-schema write must explain itself. The raw Spark error
    #    names a column and neither the table nor the cause, and the cause is
    #    almost always a table created from a 0-row Bronze file (#144).
    import contextlib

    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        bronze.explain_write_failure(
            Exception('[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] '
                      'Cannot safely cast `address` "STRING" to "INT".'),
            "silver.lake.customers",
        )
    said = out.getvalue()
    assert "silver.lake.customers" in said, said
    assert "DROP TABLE iceberg.lake.customers" in said, (
        "the message must give the exact recovery command, got: " + said
    )

    # Anything else must stay quiet rather than blame the schema.
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        bronze.explain_write_failure(Exception("connection refused"), "silver.lake.customers")
    assert out.getvalue() == "", f"unrelated errors must not be explained: {out.getvalue()!r}"

    print("bronze load contract: OK")


if __name__ == "__main__":
    main()
