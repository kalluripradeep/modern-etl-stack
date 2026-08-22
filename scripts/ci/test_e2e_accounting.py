#!/usr/bin/env python3
"""Guard the result accounting in scripts/test_transactions.py.

#149 was not a missing feature, it was a suite that reported a healthy
pipeline while two of three pipelines went unchecked. The fix is only worth
anything for as long as a skipped check keeps counting as a skip, so that
property gets a test rather than a comment.

Runs offline: importing test_transactions touches no cluster, and the paths
exercised here are the ones that fire when an endpoint is absent.
"""

import contextlib
import importlib.util
import io
import os
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent


def load():
    os.environ.pop("TRINO_URL", None)
    spec = importlib.util.spec_from_file_location(
        "tt", SCRIPTS / "test_transactions.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["tt"] = module
    spec.loader.exec_module(module)   # the __main__ guard keeps the suite from running
    return module


def run(module):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        try:
            module.summary()
        except SystemExit:
            pass
    return buf.getvalue()


def main():
    tt = load()

    # An unconfigured lakehouse is a skip, never a pass.
    assert tt.TRINO_URL == "", f"TRINO_URL should be empty, got {tt.TRINO_URL!r}"
    with contextlib.redirect_stdout(io.StringIO()):
        tt.verify_lakehouse()
    assert tt.results, "verify_lakehouse recorded nothing at all"
    assert [r[0] for r in tt.results] == ["SKIP"], tt.results

    # One skip alongside real passes must not report a healthy pipeline.
    tt.results.append(("PASS", "a real check"))
    out = run(tt)
    assert "UNVERIFIED" in out, out
    assert "all three pipelines verified" not in out, out

    # A clean sweep may claim coverage, and must name all three pipelines.
    tt.results[:] = [("PASS", "a"), ("PASS", "b")]
    out = run(tt)
    assert "all three pipelines verified" in out, out
    for pipe in ("Pipe 1", "Pipe 2", "Pipe 3"):
        assert pipe in out, f"{pipe} missing from the coverage summary:\n{out}"

    # A failure still exits non-zero and never claims coverage.
    tt.results[:] = [("PASS", "a"), ("FAIL", "b")]
    out = run(tt)
    assert "Some checks failed" in out, out
    assert "all three pipelines verified" not in out, out

    print("e2e result accounting: OK")


if __name__ == "__main__":
    main()
