#!/usr/bin/env python3
"""The seed generator must exit non-zero when it cannot seed.

It used to catch every exception, print a message and return 0. k8s/deploy.sh
runs it in a pod and decides from the exit code, so a seed that never reached
the database still printed "Sample data seeded" -- and the failure only
surfaced much later as

    Source table 'products' is missing name, description, stock_quantity

from the ingestion DAG, which points at the source data rather than at the
seed that silently did nothing (#144).

Runs offline: it points the generator at a closed port, which is a connection
failure rather than a missing dependency.
"""

import os
import subprocess  # nosec B404 - fixed argv, no shell
import sys
from pathlib import Path

SEEDER = Path(__file__).resolve().parent.parent.parent / "sample-data" / "generate_ecommerce.py"


def main():
    env = {
        **os.environ,
        "SOURCE_DB_HOST": "127.0.0.1",
        # Nothing listens here; connecting must fail rather than hang.
        "SOURCE_DB_PORT": "59999",
        "PYTHONIOENCODING": "utf-8",
    }
    proc = subprocess.run(  # nosec B603 - fixed argv, no shell
        [sys.executable, str(SEEDER)],
        capture_output=True, text=True, errors="replace", env=env, timeout=120,
    )
    assert proc.returncode != 0, (
        "the generator exited 0 with an unreachable database. deploy.sh reads "
        "this exit code to decide whether to print 'Sample data seeded', so a "
        "zero here makes a failed seed indistinguishable from a successful one."
    )
    combined = proc.stdout + proc.stderr
    assert "Error" in combined, f"the failure should say something, got: {combined[:200]!r}"
    print(f"seed exit code on failure: {proc.returncode} — OK")


if __name__ == "__main__":
    main()
