"""The benchmark ladder cannot measure an arm Postgres refuses to accept.

scripts/bench/ladder.sh runs pgbench at a list of client counts. Postgres
defaults max_connections to 100 and reserves a few of those, so a 128 client
arm never starts:

    FATAL: sorry, too many clients already
    pgbench: error: could not create connection for client 89

The ladder reports that arm as FAILED rather than 0 tps, which is correct --
nothing was measured. But the limit is ours, not the hardware's, and it sat
below the arm the manifest's own comment says to run next.

Measured on postgres:15, the setting is what decides it:

    default (100)        128 clients -> FATAL at client 89, no measurement
    max_connections=200  128 clients -> 0 failed, tps reported

200 is carried for free. At 128 live connections the container held 263MiB,
against a 2Gi limit, and 266Mi was already observed at 64 clients on the
cluster -- connections are not what consumes that memory.

Both deployments have to agree. A value set in one place is a value that
drifts: the SeaweedFS volume-slot bug (#192) was in k8s and compose alike and
went a week unnoticed because only one of them was ever read.
"""
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[2]
STATEFULSET = ROOT / "k8s" / "postgres-source" / "statefulset.yaml"
COMPOSE = ROOT / "docker-compose.yml"
LADDER = ROOT / "scripts" / "bench" / "ladder.sh"

# The largest arm the manifest's comment points at next, plus the connections
# the stack itself holds open: Debezium's replication slots, the exporter,
# health probes, and whatever shell happens to be attached.
NEEDED = 128


def max_connections(path):
    """Every max_connections value in the file, comments excluded.

    The explanations name the default of 100 in prose. Matching that would
    fail the build for describing the setting it guards.
    """
    found = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip().lstrip("-").strip().strip('"').strip("'")
        if stripped.startswith("#"):
            continue
        found += [int(m) for m in re.findall(r"max_connections=(\d+)", stripped)]
    return found


def test_both_deployments_set_it():
    for path in (STATEFULSET, COMPOSE):
        assert max_connections(path), (
            f"{path.name} does not set max_connections; Postgres defaults to "
            "100 and a 128 client ladder arm fails at client 89 before it "
            "measures anything"
        )


def test_both_deployments_agree():
    k8s = set(max_connections(STATEFULSET))
    compose = set(max_connections(COMPOSE))
    assert k8s == compose, (
        f"k8s sets max_connections={sorted(k8s)} and compose "
        f"{sorted(compose)}; a benchmark that reproduces on a laptop and not "
        "on the cluster, or the reverse, costs a day to work out"
    )


def test_it_covers_the_next_arm():
    for path in (STATEFULSET, COMPOSE):
        for value in max_connections(path):
            assert value > NEEDED, (
                f"{path.name} sets max_connections={value}, which does not "
                f"leave room above a {NEEDED} client arm. Postgres reserves "
                "connections for superusers, and the stack holds its own -- "
                "replication slots, the exporter, probes -- so the ceiling "
                "has to sit above the arm, not level with it"
            )


def test_ladder_still_reports_a_refused_arm_as_unmeasured():
    """Raising the ceiling does not remove the need to report hitting one.

    Whatever the limit is, some arm eventually exceeds it, and that arm has
    to read as a configuration limit rather than a measured collapse.
    """
    text = LADDER.read_text(encoding="utf-8")
    assert "FAILED" in text, (
        "ladder.sh no longer reports an arm that could not run as FAILED; a "
        "refused arm printed as 0 tps reads as a measured collapse"
    )


if __name__ == "__main__":
    test_both_deployments_set_it()
    test_both_deployments_agree()
    test_it_covers_the_next_arm()
    test_ladder_still_reports_a_refused_arm_as_unmeasured()
    print("source max_connections: all cases pass")
