#!/usr/bin/env python3
"""Spark and Trino against one Iceberg table: schema evolution and time travel.

scripts/test_transactions.py already proves Trino can read what the silver DAG
wrote. What it does not prove is the part that makes this a lakehouse rather
than parquet in a bucket: that two engines can take turns writing the same
table, that a schema change made by one is visible to the other without a
refresh, and that an old snapshot reads back with the schema it had at the
time rather than today's.

Those three claims are the reason the stack carries a catalog at all, and
nothing tested them.

    python scripts/test_lakehouse_interop.py

Needs TRINO_URL (scripts/test_e2e.sh exports it) and a reachable Airflow
scheduler -- the Iceberg and Postgres JARs live in the Airflow image, so the
Spark half is submitted from there. TARGET=k8s or TARGET=compose picks which,
and without it the Compose container is used when present.

The table is created and dropped by this script. It does not touch lake.orders
or depend on the silver DAG having run.
"""
import json
import os
import queue
import subprocess
import sys
import threading
import time

import requests

# Follow TARGET rather than assume Compose. scripts/test_e2e.sh forwards Trino
# to 8085 on Kubernetes, because 8082 is Compose's own Trino and may be in use
# -- and its export cannot reach a separate command, since a child script's
# environment does not propagate back to the shell that ran it. Defaulting to
# 8082 under TARGET=k8s therefore skipped every check while looking like Trino
# was down.
_DEFAULT_TRINO = ("http://localhost:8085" if os.environ.get("TARGET") == "k8s"
                  else "http://localhost:8082")
TRINO_URL = os.environ.get("TRINO_URL", _DEFAULT_TRINO)

# A submit that cannot get workers waits rather than failing, so this is a
# patience limit, not an expectation. Ten minutes is long enough for a cold
# start pulling jars and short enough that a broken cluster says so the same
# morning.
SPARK_TIMEOUT = int(os.environ.get("SPARK_TIMEOUT", "600"))
NAMESPACE = os.environ.get("NAMESPACE", "etl")
TABLE_SPARK = "silver.lake.interop_probe"
TABLE_TRINO = "iceberg.lake.interop_probe"
MARKER = "__INTEROP__"

results = []


def step(name):
    print("\n" + "=" * 60)
    print(f"  {name}")
    print("=" * 60)


def ok(msg, detail=""):
    print(f"  [PASS]  {msg}" + (f"  ({detail})" if detail else ""))
    results.append(("PASS", msg))


def fail(msg, detail=""):
    print(f"  [FAIL]  {msg}" + (f"  ({detail})" if detail else ""))
    results.append(("FAIL", msg))


def skip(msg, detail=""):
    """Record a check that could not run. Deliberately not a pass."""
    print(f"  [SKIP]  {msg}" + (f"  ({detail})" if detail else ""))
    results.append(("SKIP", msg))


def _exec_prefix():
    """Where to run spark-submit, and whether that place exists at all."""
    target = os.environ.get("TARGET")
    if target != "k8s":
        running = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True, text=True,
        ).stdout.split()
        if "airflow-scheduler" in running:
            return ["docker", "exec", "airflow-scheduler", "bash", "-c"]
        if target == "compose":
            return None
    probe = subprocess.run(
        ["kubectl", "get", "deploy", "airflow-scheduler", "-n", NAMESPACE],
        capture_output=True, text=True,
    )
    if probe.returncode != 0:
        return None
    return ["kubectl", "exec", "-n", NAMESPACE, "deploy/airflow-scheduler",
            "-c", "scheduler", "--", "bash", "-c"]


# Mirrors the catalog wiring in airflow/dags/spark_transform_silver.py. If that
# file's catalog config changes, this has to follow -- otherwise the test still
# passes while measuring a different table than the pipeline writes.
SUBMIT = r"""
spark-submit --master spark://spark-master:7077 \
  --conf spark.driver.host=$(hostname -i | cut -d" " -f1) \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.memory=1g \
  --conf spark.executor.cores=1 \
  --conf spark.hadoop.fs.s3a.endpoint=${SEAWEEDFS_ENDPOINT:-http://seaweedfs:9000} \
  --conf spark.hadoop.fs.s3a.access.key=$SEAWEEDFS_ROOT_USER \
  --conf spark.hadoop.fs.s3a.secret.key=$SEAWEEDFS_ROOT_PASSWORD \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --jars /opt/spark-jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar,/opt/spark-jars/postgresql-42.7.4.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.silver=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.silver.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog \
  --conf spark.sql.catalog.silver.uri="jdbc:postgresql://postgres-dest:5432/$DEST_DB_NAME?currentSchema=iceberg_catalog" \
  --conf spark.sql.catalog.silver.jdbc.user=$DEST_DB_USER \
  --conf spark.sql.catalog.silver.jdbc.password=$DEST_DB_PASSWORD \
  --conf spark.sql.catalog.silver.warehouse=s3a://silver/ \
  --name interop-{step} /opt/spark-jobs/lakehouse_interop.py {step}
"""


def _pump(stream, queue_):
    for line in stream:
        queue_.put(line.rstrip("\n"))
    queue_.put(None)


def spark(step_name, prefix, timeout=None):
    """Run one Spark step and return its JSON payload.

    Output is echoed as it arrives rather than captured and discarded. A
    submit that cannot get workers does not fail -- it waits, logging

        WARN TaskSchedulerImpl: Initial job has not accepted any resources;
        check your cluster UI to ensure that workers are registered

    every fifteen seconds. Capturing that produced a run which printed the
    step heading and then nothing at all for as long as the timeout allowed,
    with the one line explaining why thrown away at the end.

    The reader runs on its own thread so the deadline holds even if the
    process goes completely silent, which a per-line check would not.
    """
    timeout = timeout or SPARK_TIMEOUT
    # str.replace, not str.format: the command contains shell parameter
    # expansions like ${SEAWEEDFS_ENDPOINT:-...}, which format() reads as
    # fields of its own and rejects.
    proc = subprocess.Popen(
        prefix + [SUBMIT.replace("{step}", step_name)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
    )
    lines, q = [], queue.Queue()
    threading.Thread(target=_pump, args=(proc.stdout, q), daemon=True).start()

    deadline = time.monotonic() + timeout
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                proc.kill()
                raise RuntimeError(
                    f"spark step {step_name!r} exceeded {timeout}s. The output above "
                    "is the whole story -- repeated 'has not accepted any resources' "
                    "means the master has no usable workers. SPARK_TIMEOUT raises this."
                )
            try:
                line = q.get(timeout=min(remaining, 5))
            except queue.Empty:
                continue
            if line is None:
                break
            lines.append(line)
            if not line.startswith(MARKER):
                print(f"    | {line}", file=sys.stderr)
    except KeyboardInterrupt:
        proc.kill()
        raise
    finally:
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()

    for line in lines:
        if line.startswith(MARKER):
            return json.loads(line[len(MARKER):])
    tail = [x for x in lines if x.strip()][-3:]
    raise RuntimeError(
        f"no result from spark step {step_name!r}: " + " | ".join(tail)
    )


# nosec B608 below: every interpolation is a module constant or a snapshot
# id Iceberg itself produced. Trino identifiers cannot be bound as
# parameters anyway, so the table name has to be interpolated.

def trino(sql):
    """Run one statement over Trino's REST protocol, following nextUri."""
    r = requests.post(
        f"{TRINO_URL}/v1/statement",
        data=sql.encode("utf-8"),
        headers={"X-Trino-User": "interop-test"},
        timeout=30,
    )
    rows, cols = [], []
    while True:
        r.raise_for_status()
        page = r.json()
        if page.get("error"):
            raise RuntimeError(page["error"].get("message", "unknown Trino error"))
        cols = cols or [c["name"] for c in (page.get("columns") or [])]
        rows.extend(page.get("data") or [])
        nxt = page.get("nextUri")
        if not nxt:
            return cols, rows
        r = requests.get(nxt, headers={"X-Trino-User": "interop-test"}, timeout=30)


def run(prefix):
    step("STEP 1 - Spark creates an Iceberg table and writes to it")
    created = spark("create", prefix)
    if created["columns"] != ["id", "region", "amount"] or len(created["rows"]) != 3:
        fail("Spark did not create the table as expected", str(created)[:160])
        return False
    ok("Spark created and populated the table",
       f"{len(created['rows'])} rows, {len(created['snapshots'])} snapshot")
    snap1 = created["snapshots"][0]

    step("STEP 2 - Trino reads and aggregates the same table")
    try:
        _, agg = trino(
            f"SELECT region, count(*) n, sum(amount) total FROM {TABLE_TRINO} "  # nosec B608
            "GROUP BY region ORDER BY region"
        )
    except Exception as e:
        fail("Trino could not read the Spark-written table", str(e)[:160])
        return True
    spark_agg = [list(r) for r in created["aggregate"]]
    trino_agg = [[str(v) for v in r] for r in agg]
    if trino_agg == spark_agg:
        ok("Both engines aggregate to the same answer", str(trino_agg))
    else:
        fail("Engines disagree on the same table",
             f"spark={spark_agg} trino={trino_agg}")

    step("STEP 3 - A column added by Spark is visible to Trino")
    evolved = spark("evolve", prefix)
    if "channel" not in evolved["columns"]:
        fail("Spark did not add the column", str(evolved["columns"]))
        return True
    cols, rows = trino(f"SELECT * FROM {TABLE_TRINO} ORDER BY id")  # nosec B608
    if "channel" not in cols:
        fail("Trino cannot see the column Spark added", f"sees {cols}")
    else:
        # Pre-existing rows must read NULL. Iceberg rewrites metadata, not data
        # files, so anything else would mean the old files had been touched.
        ch = cols.index("channel")
        backfilled = [r for r in rows if r[0] in (1, 2, 3) and r[ch] is not None]
        if backfilled:
            fail("Old rows were backfilled rather than read as NULL",
                 str(backfilled)[:120])
        else:
            ok("Trino sees Spark's new column, old rows NULL",
               f"{len(cols)} columns")

    step("STEP 4 - A column added by Trino is visible to Spark")
    try:
        trino(f"ALTER TABLE {TABLE_TRINO} ADD COLUMN source_engine varchar")
        trino(f"INSERT INTO {TABLE_TRINO} VALUES "  # nosec B608
              "(5,'apac',DECIMAL '99.99','api','trino')")
    except Exception as e:
        fail("Trino could not write to the table", str(e)[:160])
        return True
    back = spark("readback", prefix)
    if "source_engine" not in back["columns"]:
        fail("Spark cannot see the column Trino added", str(back["columns"]))
    elif not any(r[-1] == "trino" for r in back["rows"]):
        fail("Spark cannot see the row Trino inserted", str(back["rows"])[:160])
    else:
        ok("Spark reads Trino's schema change and its row",
           f"{len(back['columns'])} columns, {len(back['rows'])} rows")

    step("STEP 5 - An old snapshot reads with the schema it had then")
    cols_now, rows_now = trino(f"SELECT * FROM {TABLE_TRINO} ORDER BY id")  # nosec B608
    try:
        cols_then, rows_then = trino(
            f"SELECT * FROM {TABLE_TRINO} FOR VERSION AS OF {snap1} ORDER BY id"  # nosec B608
        )
    except Exception as e:
        fail("Time travel to the first snapshot failed", str(e)[:160])
        return True
    if len(cols_then) == 3 and len(rows_then) == 3 and len(cols_now) == 5:
        ok("Time travel restores the schema, not just the rows",
           f"snapshot 1: {len(cols_then)} cols / {len(rows_then)} rows, "
           f"current: {len(cols_now)} cols / {len(rows_now)} rows")
    else:
        fail("Old snapshot did not read with its original schema",
             f"then={cols_then} now={cols_now}")

    # The column genuinely did not exist at that snapshot, so naming it has to
    # be an error. Without this, time travel could quietly degrade into a row
    # filter over today's schema and the check above would still pass.
    try:
        trino(f"SELECT channel FROM {TABLE_TRINO} FOR VERSION AS OF {snap1}")  # nosec B608
        fail("A column added later resolved against the old snapshot",
             "time travel is filtering rows, not restoring schema")
    except Exception:
        ok("A column added later cannot be resolved against the old snapshot")



    return True
def summarise():
    step("Summary")
    passed = sum(1 for r in results if r[0] == "PASS")
    failed = sum(1 for r in results if r[0] == "FAIL")
    skipped = sum(1 for r in results if r[0] == "SKIP")
    for status, msg in results:
        print(f"  [{status}]  {msg}")
    print(f"\n  Total: {passed} passed, {failed} failed, {skipped} skipped")
    if failed:
        sys.exit(1)
    if skipped:
        print("\n  Checks were skipped - interop is UNVERIFIED, not healthy.")


def main():
    prefix = _exec_prefix()
    if prefix is None:
        step("Lakehouse interop")
        skip("No Airflow scheduler reachable",
             "the Iceberg JARs live in the Airflow image; "
             "start Compose or set TARGET=k8s")
        return summarise()

    try:
        trino("SELECT 1")
    except Exception as e:
        step("Lakehouse interop")
        # Name the port-forward. "Trino is not answering" on its own sends
        # people to look at Trino, which is usually running fine -- the
        # forward just is not up, or is on the other stack's port.
        skip("Trino is not answering",
             f"{TRINO_URL} — is the port-forward up? "
             f"(bash scripts/test_e2e.sh, or kubectl port-forward -n {NAMESPACE} svc/trino 8085:8080)")
        print(f"     {str(e)[:160]}")
        return summarise()

    created = False
    try:
        created = run(prefix)
    except KeyboardInterrupt:
        # Ctrl-C used to land in the cleanup below and start a second submit
        # against the same stuck cluster, so interrupting a hung run hung
        # again. Nothing to drop if the create never returned.
        print("\n  interrupted — nothing to clean up" if not created else
              "\n  interrupted — attempting cleanup")
        if not created:
            raise
    finally:
        # Always drop, including after a failed assertion: a table left behind
        # makes the next run's CREATE the thing under test instead. Skipped
        # when nothing was created, and on a short leash either way -- cleanup
        # against a cluster that just timed out should not cost another full
        # timeout.
        if created:
            try:
                spark("drop", prefix, timeout=min(SPARK_TIMEOUT, 120))
            except Exception as e:
                print(f"  !  could not drop {TABLE_SPARK}: {str(e)[:120]}")
    summarise()


if __name__ == "__main__":
    main()
