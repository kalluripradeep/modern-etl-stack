"""A Spark submit that cannot get workers hangs instead of failing.

It is not silent about it. TaskSchedulerImpl logs

    WARN TaskSchedulerImpl: Initial job has not accepted any resources;
    check your cluster UI to ensure that workers are registered

every fifteen seconds for as long as the submit waits. That line is the whole
diagnosis, and it is logged at WARN.

The interop job used to call setLogLevel("ERROR"), which suppressed it. A
cluster whose workers were gone produced a step heading and then nothing at
all until the timeout -- ten minutes of silence with the explanation filtered
out at the source. Streaming the output did not help, because the line was
never emitted.

Measured in the scheduler image against a master with no workers:

    setLogLevel("ERROR")  ->  0 warnings in 75s, total silence
    setLogLevel("WARN")   ->  the warning every 15s, as designed

The submit sets spark.log.level=WARN as well. setLogLevel only takes effect
once the SparkContext exists, so without it every run first prints ~38 lines
of INFO startup chatter it cannot retract. WARN at both ends keeps a passing
run quiet and a stalled one explicit.
"""
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[2]
JOB = ROOT / "spark" / "jobs" / "lakehouse_interop.py"
RUNNER = ROOT / "scripts" / "test_lakehouse_interop.py"

# Levels that sit above WARN and would filter the stall warning out.
TOO_QUIET = ("ERROR", "FATAL", "OFF")


def set_log_level_calls(text):
    """Every setLogLevel argument in the file, comments excluded.

    The explanation above the call names "ERROR" in prose; matching that
    would fail the build for describing the bug it guards against.
    """
    levels = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        levels += re.findall(r"setLogLevel\(\s*[\"'](\w+)[\"']\s*\)", line)
    return levels


def test_job_does_not_silence_the_stall_warning():
    levels = set_log_level_calls(JOB.read_text(encoding="utf-8"))
    assert levels, "the interop job no longer sets a log level at all"
    for level in levels:
        assert level not in TOO_QUIET, (
            f"lakehouse_interop.py sets setLogLevel({level!r}), which filters out "
            "'Initial job has not accepted any resources'. A submit with no "
            "workers would hang to the timeout printing nothing. Use WARN."
        )


def test_submit_pins_the_startup_level():
    text = RUNNER.read_text(encoding="utf-8")
    assert "spark.log.level=WARN" in text, (
        "the submit no longer sets spark.log.level=WARN; startup INFO logging "
        "runs until the SparkContext exists, burying the run in ~38 lines per "
        "step that setLogLevel is too late to prevent"
    )


def test_submit_level_agrees_with_the_job():
    """Two knobs, one intent -- a mismatch silences one half of the run."""
    job_levels = set(set_log_level_calls(JOB.read_text(encoding="utf-8")))
    submit = re.findall(
        r"spark\.log\.level=(\w+)", RUNNER.read_text(encoding="utf-8")
    )
    assert set(submit) == job_levels, (
        f"submit sets {submit} but the job sets {sorted(job_levels)}; the "
        "stricter of the two wins and the other is decoration"
    )


def test_the_warning_text_is_still_what_we_match_on():
    """If this assertion is what broke, Spark reworded the message.

    The guard above is a level check, not a text match, so it keeps working.
    This exists so the docstrings and the #209 discussion do not quietly
    start quoting a line Spark no longer emits.
    """
    for path in (JOB, RUNNER, pathlib.Path(__file__)):
        text = path.read_text(encoding="utf-8")
        if "has not accepted any resources" in text:
            return
    raise AssertionError(
        "no file references the stall warning any more; if Spark renamed it, "
        "update the comments that explain why the level is WARN"
    )


if __name__ == "__main__":
    test_job_does_not_silence_the_stall_warning()
    test_submit_pins_the_startup_level()
    test_submit_level_agrees_with_the_job()
    test_the_warning_text_is_still_what_we_match_on()
    print("spark log level: all cases pass")
