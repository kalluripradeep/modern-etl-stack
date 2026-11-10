"""Maintenance has to be something you can tell ran. (#225)

It used to be the last task of spark_transform_silver, gated on the hour of
`data_interval_end`. The gate was correct and still cost two days on a
cluster that was filling up, because every way of checking it was misleading:

  * a triggered run gets the interval *containing* it, not the one that
    closed, so the gate saw a future hour and skipped
  * the comparison is UTC and the Airflow UI renders local time, so the run
    that does the work is not the one labelled 00:00
  * twenty-three skips a day each logged a line and exited 0, which looks
    exactly like twenty-three healthy runs

These guard the shape of the fix rather than the fix itself: maintenance is
its own scheduled DAG, and the thing it runs is not wrapped in a conditional.

Run: python scripts/ci/test_iceberg_maintenance.py
"""
import pathlib
import re
import sys

REPO = pathlib.Path(__file__).resolve().parents[2]
DAGS = REPO / 'airflow' / 'dags'
MAINTENANCE = (DAGS / 'iceberg_maintenance_dag.py').read_text(encoding='utf-8')
SILVER = (DAGS / 'spark_transform_silver.py').read_text(encoding='utf-8')
SUBMIT = (DAGS / 'spark_submit.py').read_text(encoding='utf-8')

failures = []


def check(condition, message):
    if not condition:
        failures.append(message)


def _code_only(source):
    """Strip docstrings and comments before matching.

    Both files explain the old gate at length, and a guard that reads its own
    explanation as the thing it forbids is worse than no guard: it fails on a
    correct file. This is the fourth guard in this repo to need the
    distinction, which is why it strips triple-quoted blocks too and not just
    lines beginning with #.
    """
    without_docstrings = re.sub(r'(?s)""".*?"""|\'\'\'.*?\'\'\'', '', source)
    return '\n'.join(
        line for line in without_docstrings.split('\n')
        if not line.strip().startswith('#')
    )


check(
    "schedule='@daily'" in MAINTENANCE,
    'the maintenance DAG no longer runs daily on its own schedule',
)

# The whole point: nothing decides at run time whether to do the work.
code = _code_only(MAINTENANCE)
for gate in ('data_interval_end', 'Skipping Iceberg', 'if [ ', 'logical_date'):
    check(gate not in code, f'the maintenance command is gated again on {gate!r}')

check(
    'catchup=False' in MAINTENANCE,
    'catchup is on; an outage would queue a week of compactions at once',
)
check(
    'max_active_runs=1' in MAINTENANCE,
    'two maintenance runs could rewrite the same files concurrently',
)

# Moved out of the silver DAG, not copied out of it.
check(
    'iceberg_maintenance' not in _code_only(SILVER),
    'spark_transform_silver still defines a maintenance task; it moved to '
    'iceberg_maintenance_dag.py and two of them would compact concurrently',
)

# One spark-submit builder, because a drifted copy would expire snapshots in
# a different catalogue than the one the transforms write to -- which looks
# like "maintenance ran and freed nothing", the exact symptom of #225.
for name in ('spark_submit import', 'get_spark_submit_command'):
    check(name in SILVER, f'the silver DAG no longer shares {name}')
check(
    'spark_submit import' in MAINTENANCE,
    'the maintenance DAG builds its own spark-submit instead of sharing one',
)
check(
    len(re.findall(r'def get_spark_submit_command', SUBMIT)) == 1,
    'the spark-submit builder is defined more than once',
)
check(
    'DAG(' not in SUBMIT,
    'spark_submit.py defines a DAG; it is imported by two and must not',
)

if failures:
    print('Iceberg maintenance guards failed:\n')
    for f in failures:
        print(f'  - {f}')
    sys.exit(1)

print('OK - maintenance is its own daily DAG, ungated, sharing one '
      'spark-submit builder.')
