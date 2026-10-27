"""The ladder's mirror liveness probe must tell a dead pipeline from an idle one.

A stopped CDC pipeline does not read as zero. `max(updated_at)` stops moving,
so freshness climbs at one second per second and lands in the results table as
an ordinary-looking number -- 84791, then 84852 an arm later. The throughput
figures beside it are real, which is what makes it dangerous: the run looks
like a measurement of the pipeline and is a measurement of the source alone.

Staleness on its own cannot distinguish that from an idle source, which climbs
identically and is perfectly healthy. The probe therefore commits a write and
waits for the mirror to move, and this exercises both answers with stubbed
`ch` and `pg` so that the abort path is not shipped unexecuted.
"""
import os
import pathlib
import subprocess
import tempfile
import textwrap

ROOT = pathlib.Path(__file__).resolve().parents[2]
LADDER = ROOT / "scripts" / "bench" / "ladder.sh"


def run_probe(timestamps, timeout=3):
    """Drive mirror_is_live() with a stub `ch` that returns `timestamps` in turn.

    Sourcing with LADDER_LIB_ONLY stops the script before it runs anything,
    leaving the function defined. The stubs are declared afterwards so they
    replace the real ones.
    """
    # Both temp files live in the working directory and are referred to by
    # relative name. Git Bash cannot open a "C:/..." path, so anything handed
    # to bash -- the script itself, or the counter path written inside it --
    # has to be something bash can resolve, and a relative name needs no
    # translation on any platform.
    fd, counter_abs = tempfile.mkstemp(prefix="ladder-probe-", dir=ROOT)
    os.close(fd)
    counter = pathlib.Path(counter_abs).name
    script = textwrap.dedent(f"""
        export LADDER_LIB_ONLY=1
        export LIVENESS_TIMEOUT={timeout}
        # ladder.sh picks a target and would try to reach a cluster; TARGET
        # only decides which stub-able functions it defines, and LIB_ONLY
        # returns before any of them are called.
        export TARGET=compose
        # Relative, and run from the repo root: an absolute Windows path is
        # not something bash can source, and silencing the error here once
        # cost an afternoon of a guard that reported DEAD for every case
        # because the function under test had never been defined.
        source scripts/bench/ladder.sh
        [ "$(type -t mirror_is_live)" = "function" ] || {{ echo "SOURCE-FAILED"; exit 1; }}

        # The counter lives in a file, not a variable: mirror_is_live reads ch
        # through $( ), which runs it in a subshell, so a variable increment
        # would be discarded and every call would return the first value.
        #
        # Python supplies the path rather than mktemp, which returns empty
        # under a bash spawned from Python on Windows and leaves every read
        # failing on an empty filename.
        STAMPS=({" ".join(repr(t) for t in timestamps)})
        COUNTER={counter!r}
        echo 0 > "$COUNTER"
        ch() {{
          local i; i=$(cat "$COUNTER")
          echo "${{STAMPS[$i]:-}}"
          echo $((i + 1)) > "$COUNTER"
        }}
        pg() {{ return 0; }}
        sleep() {{ return 0; }}   # keep the test instant

        if mirror_is_live; then echo LIVE; else echo DEAD; fi
    """)
    # Run from a file rather than `bash -c`. Passing a long multi-line script
    # as an argument goes through MSYS argument conversion on Windows, which
    # rewrites anything that looks like a path inside it -- the counter path
    # arrived as an empty string and every read failed on `cat ""`.
    fd2, script_abs = tempfile.mkstemp(prefix="ladder-probe-", suffix=".sh", dir=ROOT)
    os.close(fd2)
    pathlib.Path(script_abs).write_text(script, encoding="utf-8", newline="\n")
    out = subprocess.run(
        ["bash", pathlib.Path(script_abs).name],
        capture_output=True, text=True, cwd=ROOT,
    )
    os.unlink(script_abs)
    os.unlink(counter_abs)
    lines = out.stdout.strip().splitlines()
    if not lines:
        raise AssertionError(f"probe produced no output; stderr: {out.stderr[-300:]}")
    if lines[-1] == "SOURCE-FAILED":
        raise AssertionError(f"ladder.sh did not define mirror_is_live; stderr: {out.stderr[-300:]}")
    return lines[-1]


def test_moving_mirror_is_live():
    # before=T1, then the mirror advances on the first poll.
    assert run_probe(["2026-01-01T00:00:00", "2026-01-01T00:00:05"]) == "LIVE"


def test_frozen_mirror_is_dead():
    # The mirror never moves -- Raghu's case, where it had been stuck 23 hours.
    assert run_probe(["2026-01-01T00:00:00"] * 6) == "DEAD"


def test_idle_source_that_later_commits_is_live():
    """An idle source is not a dead pipeline.

    Staleness is large and climbing in both cases, which is exactly why the
    probe asks whether a write lands rather than how old the data is. Here the
    mirror is stale but does move, two polls in.
    """
    assert run_probe([
        "2026-01-01T00:00:00",   # before
        "2026-01-01T00:00:00",   # still nothing
        "2026-01-01T00:00:00",   # still nothing
        "2026-01-02T03:04:05",   # the write lands
    ]) == "LIVE"


def test_empty_mirror_does_not_count_as_movement():
    """An unreadable mirror must not be mistaken for a live one.

    If ClickHouse answers with nothing, `after` is empty and differs from
    `before` as a string -- which would read as movement without the explicit
    non-empty test.
    """
    assert run_probe(["2026-01-01T00:00:00", "", "", ""]) == "DEAD"


if __name__ == "__main__":
    test_moving_mirror_is_live()
    test_frozen_mirror_is_dead()
    test_idle_source_that_later_commits_is_live()
    test_empty_mirror_does_not_count_as_movement()
    print("ladder liveness probe: all cases pass")
