"""The interop test's Spark runner must not go silent, and must not hang twice.

A submit that cannot get workers does not fail. It waits, logging

    WARN TaskSchedulerImpl: Initial job has not accepted any resources;
    check your cluster UI to ensure that workers are registered

every fifteen seconds. Captured, that produced a run which printed its step
heading and then nothing for ten minutes, with the one line explaining why
discarded at the end -- and a Ctrl-C that landed in cleanup and started a
second submit against the same stuck cluster, hanging again.

These drive spark() with a stub command instead of a cluster, so the three
behaviours that matter are exercised without one: output reaches the terminal
while the process is still running, the deadline is enforced even in silence,
and a marker is still parsed out of noisy output.
"""
import importlib.util
import pathlib
import subprocess
import sys
import types

ROOT = pathlib.Path(__file__).resolve().parents[2]
TARGET = ROOT / "scripts" / "test_lakehouse_interop.py"


def load():
    """Import the script without running it.

    It imports `requests`, which CI installs for the e2e job but which this
    guard has no other need of, so a stub stands in when it is absent.
    """
    if "requests" not in sys.modules:
        try:
            import requests  # noqa: F401
        except ImportError:
            sys.modules["requests"] = types.ModuleType("requests")
    spec = importlib.util.spec_from_file_location("interop", TARGET)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def shell_prefix():
    """A prefix that runs the submit text as a shell script, not as Spark."""
    return [sys.executable, "-c",
            "import sys,subprocess;sys.exit(subprocess.run(['bash','-c',sys.argv[1]]).returncode)"]


def test_marker_is_parsed_out_of_noise():
    mod = load()
    mod.SUBMIT = (
        'echo "noise before"; '
        'echo "%s {\\"columns\\": [\\"id\\"], \\"rows\\": [], \\"snapshots\\": []}"; '
        'echo "noise after"' % mod.MARKER
    )
    out = mod.spark("create", shell_prefix(), timeout=30)
    assert out["columns"] == ["id"], out


def test_output_is_echoed_not_swallowed(capsys_path=None):
    """The point of the fix: Spark's own explanation must reach the user."""
    mod = load()
    mod.SUBMIT = (
        'echo "Initial job has not accepted any resources"; '
        'echo "%s {\\"columns\\": [], \\"rows\\": [], \\"snapshots\\": []}"' % mod.MARKER
    )
    proc = subprocess.run(
        [sys.executable, "-c", (
            "import importlib.util,sys,types\n"
            "sys.modules.setdefault('requests', types.ModuleType('requests'))\n"
            f"spec=importlib.util.spec_from_file_location('m', r'{TARGET}')\n"
            "m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)\n"
            "m.SUBMIT='echo \"Initial job has not accepted any resources\"; "
            "echo \"'+m.MARKER+' {}\"'\n"
            "m.spark('create', ['bash','-c'], timeout=30)\n"
        )],
        capture_output=True, text=True, cwd=ROOT,
    )
    assert "has not accepted any resources" in proc.stderr, (
        "spark() swallowed the one line that explains a stalled submit:\n"
        + proc.stderr[-400:]
    )


def test_deadline_holds_when_the_process_is_silent():
    """A per-line check would block forever here; the reader thread must not."""
    mod = load()
    mod.SUBMIT = "sleep 60"
    try:
        mod.spark("create", shell_prefix(), timeout=3)
    except RuntimeError as e:
        assert "exceeded 3s" in str(e), e
    else:
        raise AssertionError("a silent process ran past its deadline")


def test_missing_marker_reports_the_tail():
    mod = load()
    mod.SUBMIT = 'echo "something went wrong"; echo "and then this"'
    try:
        mod.spark("create", shell_prefix(), timeout=30)
    except RuntimeError as e:
        assert "and then this" in str(e), e
    else:
        raise AssertionError("a submit with no marker was treated as success")


if __name__ == "__main__":
    test_marker_is_parsed_out_of_noise()
    test_output_is_echoed_not_swallowed()
    test_deadline_holds_when_the_process_is_silent()
    test_missing_marker_reports_the_tail()
    print("interop spark runner: all cases pass")
