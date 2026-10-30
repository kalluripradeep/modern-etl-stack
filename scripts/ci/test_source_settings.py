"""The source Postgres has to be told about the container it runs in.

Two settings, one shape of bug. Both deployments start postgres with a list
of `-c key=value` arguments, and anything not in that list runs at the
upstream default regardless of what the manifest's limits say.

max_connections (#188). The default is 100 and some of those are reserved,
so a 128 client ladder arm failed at client 89 with

    FATAL: sorry, too many clients already

before a single transaction ran -- a configuration limit reported as a failed
measurement. scripts/bench/ladder.sh calls that arm FAILED rather than 0 tps,
which is correct, but the limit was ours.

shared_buffers (#188 again). The source never set it, so it ran the 128MB
default under a 2Gi limit while postgres-dest has had 256MB since it was
written. That is why the container measured a flat 263MiB whether 64 or 128
clients were connected: the number had nothing to do with the clients.

work_mem is deliberately absent and this file keeps it that way. The
destination sets 64MB, which suits a few dbt connections doing bulk loads.
Here max_connections is 200, and work_mem is allocated per sort per
connection -- the same value is an order of magnitude more worst case than
the 2Gi limit allows. A ceiling that can be exceeded by configuration is not
a ceiling.

Both deployments have to carry the same list. A value set on one side only is
exactly the shape the SeaweedFS volume-slot bug had (#192): wrong in k8s and
compose alike, unnoticed for a week because only one of them was ever read.
"""
import pathlib

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]
STATEFULSET = ROOT / "k8s" / "postgres-source" / "statefulset.yaml"
COMPOSE = ROOT / "docker-compose.yml"
LADDER = ROOT / "scripts" / "bench" / "ladder.sh"

# The largest arm the manifest's comment points at next, plus the connections
# the stack itself holds open: Debezium's replication slots, the exporter,
# health probes, and whatever shell happens to be attached.
NEEDED = 128


def load(path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def k8s_args():
    containers = load(STATEFULSET)["spec"]["template"]["spec"]["containers"]
    return next(c for c in containers if c["name"] == "postgres")["args"]


def compose_args():
    return load(COMPOSE)["services"]["postgres-source"]["command"]


def settings(args):
    """The `-c key=value` arguments, as a dict.

    Parsing the YAML rather than the text means the explanations above these
    lines -- which name the defaults in prose -- are dropped for free.
    """
    pairs = [str(a).split("=", 1) for a in args if "=" in str(a)]
    return {key: value for key, value in pairs}


DEPLOYMENTS = {"statefulset.yaml": k8s_args, "docker-compose.yml": compose_args}


def test_both_deployments_agree():
    k8s = settings(k8s_args())
    compose = settings(compose_args())
    assert k8s == compose, (
        f"the two deployments start postgres differently.\n"
        f"  only in k8s:     {sorted(set(k8s.items()) - set(compose.items()))}\n"
        f"  only in compose: {sorted(set(compose.items()) - set(k8s.items()))}\n"
        "A benchmark that reproduces on a laptop and not on the cluster, or "
        "the reverse, costs a day to work out."
    )


def test_both_deployments_set_max_connections():
    for name, args in DEPLOYMENTS.items():
        assert "max_connections" in settings(args()), (
            f"{name} does not set max_connections; Postgres defaults to 100 "
            "and a 128 client ladder arm fails at client 89 before it "
            "measures anything"
        )


def test_max_connections_covers_the_next_arm():
    for name, args in DEPLOYMENTS.items():
        value = settings(args()).get("max_connections")
        if value is None:
            continue
        assert int(value) > NEEDED, (
            f"{name} sets max_connections={value}, which does not leave room "
            f"above a {NEEDED} client arm. Postgres reserves connections for "
            "superusers, and the stack holds its own -- replication slots, "
            "the exporter, probes -- so the ceiling has to sit above the arm, "
            "not level with it"
        )


def test_both_deployments_set_shared_buffers():
    for name, args in DEPLOYMENTS.items():
        assert "shared_buffers" in settings(args()), (
            f"{name} does not set shared_buffers, so postgres uses the 128MB "
            "default no matter what the memory limit says. The container then "
            "reports the same resident size under any load, which reads as a "
            "workload that does not need memory rather than a server that was "
            "never told it had any"
        )


def test_work_mem_is_left_at_its_default():
    """Not an oversight -- see the note at the top of this file.

    If you are here because this failed: work_mem is per sort, per
    connection, and max_connections is 200. Raising it needs the memory limit
    and the connection ceiling considered together, which is a change worth
    making deliberately rather than by copying postgres-dest.
    """
    for name, args in DEPLOYMENTS.items():
        found = settings(args())
        assert "work_mem" not in found, (
            f"{name} sets work_mem={found['work_mem']} on a server that "
            f"allows {found.get('max_connections', '?')} connections. That is "
            "allocated per sort per connection, so the worst case is the "
            "product of the two -- check it against the memory limit before "
            "removing this guard"
        )


def test_ladder_still_reports_a_refused_arm_as_unmeasured():
    """Raising a ceiling does not remove the need to report hitting one.

    Whatever the limit is, some arm eventually exceeds it, and that arm has
    to read as a configuration limit rather than a measured collapse.
    """
    text = LADDER.read_text(encoding="utf-8")
    assert "FAILED" in text, (
        "ladder.sh no longer reports an arm that could not run as FAILED; a "
        "refused arm printed as 0 tps reads as a measured collapse"
    )


if __name__ == "__main__":
    test_both_deployments_agree()
    test_both_deployments_set_max_connections()
    test_max_connections_covers_the_next_arm()
    test_both_deployments_set_shared_buffers()
    test_work_mem_is_left_at_its_default()
    test_ladder_still_reports_a_refused_arm_as_unmeasured()
    print("source settings: all cases pass")
