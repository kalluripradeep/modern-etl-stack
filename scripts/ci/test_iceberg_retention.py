"""The snapshot retention window is a disk budget, not a preference. (#225)

The silver transforms use MERGE INTO, and Iceberg's default copy-on-write
rewrites every data file containing a matched row. On a small dimension that
is the whole table, every run. So an hourly pipeline leaves a complete new
generation behind every hour, and the expiry window decides how many are
kept -- seven days meant roughly 168.

That is not a theory. One catch-up run on the cluster in #225 expired 2208
data files from a fifty-row `customers` table, and the node had already hit
DiskPressure and started evicting pods twice.

So the window is load-bearing, and widening it back is a storage decision
rather than a tidy-up. These guards make that decision visible in review.

Run: python scripts/ci/test_iceberg_retention.py
"""
import datetime
import pathlib
import re
import sys

REPO = pathlib.Path(__file__).resolve().parents[2]
JOB = REPO / 'spark' / 'jobs' / 'iceberg_maintenance.py'
SOURCE = JOB.read_text(encoding='utf-8')

failures = []


def check(condition, message):
    if not condition:
        failures.append(message)


def _code_only(source):
    """Strip comment lines only -- deliberately NOT triple-quoted strings.

    The sibling guard in test_iceberg_maintenance.py strips docstrings too,
    because there the prose explaining the old gate matched as the gate. Here
    the opposite holds: the thing being guarded *is* a triple-quoted string,
    since the expire_snapshots call is written as spark.sql(f\"\"\"...\"\"\").
    Stripping those would delete the SQL and report a correct file as broken,
    which is what the first version of this file did.

    Comments are enough. They discuss the old window in words ("seven days"),
    never in the code forms matched below.
    """
    return '\n'.join(
        line for line in source.split('\n')
        if not line.strip().startswith('#')
    )


code = _code_only(SOURCE)

default = re.search(
    r"ICEBERG_RETENTION_DAYS'\s*,\s*'(\d+)'", code
)
check(default is not None,
      'the expiry window is no longer read from ICEBERG_RETENTION_DAYS')

if default:
    days = int(default.group(1))
    check(days <= 2,
          f'the default expiry window is {days} days. At an hourly cadence '
          f'that retains ~{days * 24} generations of every table, and #225 '
          f'filled a node disk at 7. Raise it deliberately or not at all.')
    check(days >= 1,
          f'the default expiry window is {days} days, which leaves no room '
          'to recover from a bad run noticed the next day')

# The window has to reach the SQL. A constant left behind in the CALL is the
# failure mode that looks fixed and is not.
check('days=retention_days' in code.replace(' ', ''),
      'expire_snapshots no longer uses the configured window')
check(not re.search(r'timedelta\(days=7\)', code),
      'a hardcoded seven-day window is back in the expiry call')

# The floor under a table nobody writes any more.
check('retain_last => 5' in code,
      'retain_last is gone; a quiet table could expire to no snapshots at all')

# Orphan removal is the other half: expire_snapshots drops the metadata
# pointers, orphans are what a failed write left behind, and only this
# clears them.
check('remove_orphan_files' in code,
      'orphan removal is gone; expiry alone does not clear failed writes')

# The format string must stay something Spark can parse as a TIMESTAMP.
stamp = (datetime.datetime(2026, 11, 11, 3, 4, 5)
         - datetime.timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S')
check(re.fullmatch(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', stamp) is not None,
      'the timestamp format no longer matches what expire_snapshots expects')
check("'%Y-%m-%d %H:%M:%S'" in code,
      'the expiry timestamp format changed; Spark parses this as a TIMESTAMP')

if failures:
    print('Iceberg retention guards failed:\n')
    for f in failures:
        print(f'  - {f}')
    sys.exit(1)

print(f'OK - snapshot expiry defaults to {default.group(1)} days, '
      'configurable, with retain_last and orphan removal intact.')
