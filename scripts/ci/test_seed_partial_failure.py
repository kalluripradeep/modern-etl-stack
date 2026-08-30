#!/usr/bin/env python3
"""The seed must survive a bad row, and must not overstate what it wrote.

PostgreSQL aborts the whole transaction on any failed statement, so the
original `except: continue` did not skip one row -- every later insert failed
with "current transaction is aborted, commands ignored until end of
transaction block", the commit wrote nothing, and the script still printed
"Generated 100 customers". One duplicate email emptied the entire seed while
reporting success, which is how a cluster re-seeded repeatedly and kept its
old schema (#144).

Two properties are pinned here: the loop rolls back to a savepoint rather than
carrying on inside a doomed transaction, and the count printed is the count
actually written.
"""

import importlib.util
import io
import contextlib
import sys
from pathlib import Path

SEEDER = Path(__file__).resolve().parent.parent.parent / "sample-data" / "generate_ecommerce.py"


def load():
    spec = importlib.util.spec_from_file_location("seeder", SEEDER)
    module = importlib.util.module_from_spec(spec)
    sys.modules["seeder"] = module
    spec.loader.exec_module(module)   # __main__ guard keeps main() from running
    return module


def main():
    source = SEEDER.read_text(encoding="utf-8")

    # A bare `continue` inside a transaction is the bug; the rollback is the fix.
    assert "ROLLBACK TO SAVEPOINT" in source, (
        "the insert loop must roll back to a savepoint on a failed row. Without "
        "it PostgreSQL leaves the transaction aborted and every later insert "
        "fails, so one bad row silently empties the whole seed."
    )
    assert "SAVEPOINT row" in source, "no savepoint is being taken per row"

    seeder = load()

    # Nothing written must be fatal: every later step depends on this data.
    out = io.StringIO()
    try:
        with contextlib.redirect_stdout(out):
            seeder._report("customers", 0, 100)
    except SystemExit as e:
        assert e.code != 0, "writing nothing must exit non-zero"
    else:
        raise AssertionError("_report wrote 0 rows and did not exit")
    assert "0" in out.getvalue(), out.getvalue()

    # A partial write must say so rather than claim the requested count.
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        seeder._report("customers", 1, 100)
    text = out.getvalue()
    assert "1" in text and "99" in text, (
        f"a partial seed must report what was written and what failed, got: {text!r}"
    )
    assert "Generated 100 customers" not in text, (
        "the requested count must not be reported as the written count"
    )

    # A full write still reads as success.
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        seeder._report("customers", 100, 100)
    assert "100" in out.getvalue(), out.getvalue()

    print("seed partial-failure reporting: OK")


if __name__ == "__main__":
    main()
