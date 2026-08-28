#!/usr/bin/env python3
"""Guard the COPY path against both psycopg drivers.

apache-airflow-providers-postgres is floored but not capped, and its 6.x line
moved from psycopg2 to psycopg3, where copy_expert does not exist:

    AttributeError: 'Cursor' object has no attribute 'copy_expert'

That reached a cluster (#144) as a failed ingest_orders task while the other
three tables failed on something else entirely, so it was easy to read as one
problem. The same class of break is already documented for pyspark in
requirements-airflow.txt: a floor without a cap is a bomb with someone else's
release schedule on the timer.

Both drivers are exercised here with fakes, so neither has to be installed.
"""

import ast
import sys
from pathlib import Path

DAG = Path(__file__).resolve().parent.parent.parent / "airflow" / "dags" / "ingest_source_to_bronze.py"


class Psycopg2Cursor:
    """Has copy_expert; no copy()."""

    def __init__(self):
        self.called_with = None

    def copy_expert(self, sql, buf):
        self.called_with = (sql, buf.read())


class Psycopg3Copy:
    def __init__(self, cursor, sql):
        self.cursor, self.sql = cursor, sql

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def write(self, data):
        self.cursor.called_with = (self.sql, data)


class Psycopg3Cursor:
    """Has copy(); no copy_expert."""

    def __init__(self):
        self.called_with = None

    def copy(self, sql):
        return Psycopg3Copy(self, sql)


def extract_copy_block():
    """Pull the driver-dispatch block out of the DAG without importing Airflow."""
    src = DAG.read_text(encoding="utf-8")
    tree = ast.parse(src)
    import textwrap

    for node in ast.walk(tree):
        if isinstance(node, ast.If) and "copy_expert" in ast.dump(node.test):
            # get_source_segment strips the first line's indent but not the
            # rest, so the block is unparseable until it is squared up.
            segment = ast.get_source_segment(src, node)
            return textwrap.dedent(" " * node.col_offset + segment)
    raise AssertionError("no copy_expert capability check found in the ingest DAG")


def main():
    import io

    block = extract_copy_block()
    assert "copy_expert" in block and ".copy(" in block, (
        "the block must handle both drivers, got:\n" + block
    )

    for name, cursor in (("psycopg2", Psycopg2Cursor()), ("psycopg3", Psycopg3Cursor())):
        scope = {"pg_cursor": cursor, "copy_sql": "COPY t (a) FROM STDIN WITH CSV",
                 "buffer": io.StringIO("1\n2\n")}
        exec(block, scope)  # nosec B102 - the block comes from this repo's own source
        assert cursor.called_with is not None, f"{name}: COPY was never issued"
        sql, data = cursor.called_with
        assert sql.startswith("COPY t"), f"{name}: wrong statement {sql!r}"
        assert data == "1\n2\n", f"{name}: wrong payload {data!r}"
        print(f"  ok    {name}: {sql}")

    print("COPY driver compatibility: OK")


if __name__ == "__main__":
    sys.exit(main())
