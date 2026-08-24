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

    # The silver-trigger step decides whether Pipe 2 gets a chance to run at
    # all before it is asserted. Its branches need a cluster to exercise, so
    # they are pinned here with fakes instead.
    from unittest import mock

    def outcome(cli, states):
        """Run run_silver_pipeline with a canned CLI and state sequence."""
        tt.results[:] = []
        seq = list(states)
        # DAG_WAIT_SECONDS is pinned short. Left at its 900s default the
        # timeout case spins on real wall-clock -- sleep is mocked, time.time
        # is not -- so this would only finish quickly if the caller happened
        # to export SILVER_DAG_TIMEOUT. A check that needs the environment to
        # terminate is not a check.
        fake_proc = mock.Mock(returncode=0, stdout="", stderr="")
        with (
            mock.patch.object(tt, "DAG_WAIT_SECONDS", 1),
            mock.patch.object(tt, "_airflow_cli", return_value=cli),
            mock.patch.object(
                tt, "_runs",
                side_effect=lambda _: seq.pop(0) if seq else [],
            ),
            mock.patch.object(tt.subprocess, "run", return_value=fake_proc),
            mock.patch.object(tt.time, "sleep"),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            tt.run_silver_pipeline()
        return [r[0] for r in tt.results]

    CLI = ["fake", "airflow"]
    assert outcome(None, []) == ["SKIP"], "no Airflow reachable must skip, not pass"
    assert outcome(CLI, [[], [{"state": "success"}]]) == ["PASS"], "a drained queue ending in success must pass"
    assert outcome(CLI, [[], [{"state": "failed"}]]) == ["FAIL"], "a failed DAG must fail"
    # Never draining is a failure, not a pass: Pipe 2 is unverified either way.
    assert outcome(CLI, [[{"state": "queued"}]] * 50) == ["FAIL"], "a queue that never drains must fail"

    # The reason #155 timed out on a healthy cluster: it triggered a run behind
    # an existing backlog and then waited for that run, so the step blocked on
    # the whole queue. With work already pending it must wait, not pile on.
    tt.results[:] = []
    seq = [[{"state": "queued"}, {"state": "running"}],
           [{"state": "success"}, {"state": "success"}]]
    with (
        mock.patch.object(tt, "DAG_WAIT_SECONDS", 1),
        mock.patch.object(tt, "_airflow_cli", return_value=CLI),
        mock.patch.object(tt, "_runs", side_effect=lambda _: seq.pop(0) if seq else []),
        mock.patch.object(tt.subprocess, "run") as spawned,
        mock.patch.object(tt.time, "sleep"),
        contextlib.redirect_stdout(io.StringIO()),
    ):
        tt.run_silver_pipeline()
    assert spawned.call_count == 0, "must not trigger when a run is already pending"
    assert [r[0] for r in tt.results] == ["PASS"], tt.results

    # A wedged run and a deep queue look identical from outside -- both report
    # "queued" forever -- but the remedies are opposite. The message has to
    # tell them apart, which is the round trip #144 spent.
    day_old = [{"state": "running", "start_date": "2026-08-15T00:00:00+00:00"},
               {"state": "queued", "start_date": ""}]
    detail = tt._stuck_detail(day_old)
    assert "stuck run" in detail, detail
    assert "Mark it failed" in detail, detail
    assert "raise SILVER_DAG_TIMEOUT" not in detail, (
        "a nine-day-old run is not a backlog; telling someone to wait longer "
        f"is the wrong remedy: {detail}"
    )

    from datetime import datetime, timezone
    fresh = [{"state": "queued",
              "start_date": datetime.now(timezone.utc).isoformat()}]
    detail = tt._stuck_detail(fresh)
    assert "backlog drains" in detail, detail
    assert "stuck run" not in detail, detail

    print("e2e result accounting: OK")


if __name__ == "__main__":
    main()
