"""The connector's TASK state is what matters, and it is not the connector's.

Kafka Connect accepts the registration POST, reports the CONNECTOR as RUNNING,
and only then starts its task. If that task dies on its first poll it goes
FAILED and stays there -- Connect does not retry a failed task -- while the
connector goes on reporting RUNNING. A cluster ran for a day on exactly that:
a mirror nothing was writing to, and every status check anyone ran saying
RUNNING.

register_debezium_connector.sh now reads the task state and fails if it is not
RUNNING. It reads it positionally, because jq is not in every image this runs
from, and "state" appears twice in the reply -- connector first, task second.

That extraction is the fragile part: a change to Kafka Connect's field order
would silently start reporting the connector's state instead, which is always
RUNNING, and the check would pass for every broken cluster. These run the
exact pipeline from the script against the exact JSON Connect returns.
"""
import json
import pathlib
import re
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "register_debezium_connector.sh"

FAILED_TRACE = (
    "io.debezium.DebeziumException: Couldn't obtain encoding for database sourcedb\n"
    "\tat io.debezium.connector.postgresql.connection.PostgresConnection"
    ".getDatabaseCharset(PostgresConnection.java:577)\n"
    "Caused by: java.net.UnknownHostException: "
    "postgres-source-0.postgres-source.etl.svc.cluster.local\n"
)


def connect_status(task_state, compact=True):
    """Shaped exactly like Kafka Connect's reply, connector RUNNING regardless.

    Connect returns compact JSON. The spaced variant is kept as a case of its
    own because the extraction is a text pattern, and a pattern that only
    matched one spelling would be a trap waiting for a Connect release.
    """
    task = {"id": 0, "state": task_state, "worker_id": "null:-1"}
    if task_state == "FAILED":
        task["trace"] = FAILED_TRACE
    doc = {
        "name": "orders-cdc-connector",
        "connector": {"state": "RUNNING", "worker_id": "null:-1"},
        "tasks": [task],
        "type": "source",
    }
    return json.dumps(doc, separators=(",", ":") if compact else (", ", ": "))


def extraction_pipeline():
    """Lift the parsing out of the script so the test cannot drift from it.

    Taking it from the file rather than restating it is the point: a guard
    holding its own copy of the pipeline would keep passing after the script's
    copy changed.
    """
    text = SCRIPT.read_text(encoding="utf-8")
    m = re.search(r"\|\s*(grep -oE? '[^']*state[^']*'\s*\|\s*sed[^|]*\|\s*cut[^\n]*)", text)
    assert m, "could not find the task-state extraction in the script"
    return m.group(1).strip()


def extract(task_state, compact=True):
    pipeline = extraction_pipeline()
    out = subprocess.run(
        ["bash", "-c", f"cat | {pipeline}"],
        input=connect_status(task_state, compact),
        capture_output=True, text=True, timeout=60,
    )
    return out.stdout.strip()


def test_failed_task_is_not_read_as_the_connectors_running():
    """The case that cost a day: connector RUNNING, task FAILED."""
    assert extract("FAILED") == "FAILED", (
        "the task state was read as the connector's, which is always RUNNING -- "
        "every broken cluster would pass this check"
    )


def test_running_task_reads_as_running():
    assert extract("RUNNING") == "RUNNING"


def test_whitespace_in_the_reply_does_not_break_it():
    assert extract("FAILED", compact=False) == "FAILED",         "a space after the colon defeated the pattern"


def test_other_states_are_not_silently_running():
    for state in ("UNASSIGNED", "PAUSED", "RESTARTING"):
        assert extract(state) == state, f"{state} was misread"


def test_script_treats_only_running_as_success():
    """Whatever comes out, only RUNNING may exit 0."""
    text = SCRIPT.read_text(encoding="utf-8")
    assert 'if [ "$TASK_STATE" = "RUNNING" ]; then' in text, \
        "the success branch no longer tests for RUNNING"
    assert "exit 1" in text.split('if [ "$TASK_STATE" = "RUNNING" ]; then')[1], \
        "a non-RUNNING task no longer fails the script"


def test_existing_connector_still_reaches_the_check():
    """Re-running against a dead task is the one case worth not skipping.

    The script used to print "already exists" and exit 0, so the single
    situation in which anyone runs it twice was the one it refused to look at.
    """
    text = SCRIPT.read_text(encoding="utf-8")
    before_check = text.split("task_state()")[0]
    assert "exit 0" not in before_check, (
        "an early exit was reintroduced before the task check; an existing "
        "connector with a dead task would be reported as success"
    )


if __name__ == "__main__":
    test_failed_task_is_not_read_as_the_connectors_running()
    test_running_task_reads_as_running()
    test_whitespace_in_the_reply_does_not_break_it()
    test_other_states_are_not_silently_running()
    test_script_treats_only_running_as_success()
    test_existing_connector_still_reaches_the_check()
    print("connector task check: all cases pass")
