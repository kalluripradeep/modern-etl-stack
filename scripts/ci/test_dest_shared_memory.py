"""The destination Postgres needs more than the container default /dev/shm.

Parallel query workers pass rows through POSIX shared memory, and postgres
allocates those segments under /dev/shm. Containers get 64MB there by
default, from the runtime rather than from anything in our manifests, so it
is a ceiling nobody set and nobody can see in the pod spec.

The statefulset sets work_mem=64MB, which sizes each worker's hash table
against a budget the workers cannot reach. The three-table join in
scripts/benchmark.py asks for the most of it and was the one cell that failed
on the cluster (#258):

    could not resize shared memory segment "/PostgreSQL.627441648"
    to 16777216 bytes

That is a configuration limit reported as a failed measurement, which is what
max_connections did to the ladder in #188.

The tmpfs counts against the container's memory limit, so the size is not a
free number: it has to leave postgres room to hit its own limits first.

Both deployments have to carry it. A value set on one side only is the shape
of #192, wrong in k8s and compose alike and unnoticed because only one of
them was ever read.
"""
import pathlib
import re

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]
STATEFULSET = ROOT / "k8s" / "postgres-dest" / "statefulset.yaml"
COMPOSE = ROOT / "docker-compose.yml"

# nosec B108 -- a container mount path we assert on, not a tmp file we write.
MOUNT = "/dev/shm"  # nosec B108
# Bigger than the container default it exists to replace, or it buys nothing.
DEFAULT_SHM = 64 * 1024**2


def to_bytes(value):
    """Parse the size spellings k8s and compose each accept."""
    m = re.fullmatch(r"(\d+)\s*([KMGT]i|[kmgt]b|[KMGT])?", str(value).strip())
    assert m, f"unparseable size: {value!r}"
    unit = (m.group(2) or "").lower().rstrip("ib")
    return int(m.group(1)) * 1024 ** ("kmgt".index(unit) + 1 if unit else 0)


def shm():
    """The postgres container, its /dev/shm mount, and the volume behind it."""
    spec = yaml.safe_load(STATEFULSET.read_text(encoding="utf-8"))["spec"]["template"]["spec"]
    pg = next(c for c in spec["containers"] if c["name"] == "postgres")
    mount = next((m for m in pg["volumeMounts"] if m["mountPath"] == MOUNT), None)
    assert mount, f"postgres-dest has no {MOUNT} mount — parallel joins run on the 64MB default"
    return pg, next(v for v in spec["volumes"] if v["name"] == mount["name"])


def test_the_workers_get_shared_memory_the_runtime_would_not_have_given_them():
    _, vol = shm()
    assert vol.get("emptyDir", {}).get("medium") == "Memory", (
        f"{MOUNT} must be a Memory emptyDir — a disk-backed one is slower than "
        "the default it replaces"
    )
    assert to_bytes(vol["emptyDir"]["sizeLimit"]) > DEFAULT_SHM, (
        f"{MOUNT} is no larger than the 64MB container default it exists to raise"
    )


def test_the_tmpfs_cannot_eat_the_container_before_postgres_does():
    pg, vol = shm()
    size = to_bytes(vol["emptyDir"]["sizeLimit"])
    limit = to_bytes(pg["resources"]["limits"]["memory"])
    # tmpfs pages are charged to the container, so a generous sizeLimit turns
    # a runaway query into an OOMKill of the whole pod rather than a failed
    # query. Half the limit is already more than parallel workers should need.
    assert size * 2 <= limit, (
        f"{MOUNT} at {size} is over half the {limit} memory limit — "
        "filling it would OOMKill postgres instead of failing the query"
    )


def test_both_deployments_set_it():
    svc = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))["services"]["postgres-dest"]
    assert "shm_size" in svc, (
        "docker-compose postgres-dest has no shm_size — the cluster was fixed "
        "and this side was not, which is how #192 stayed hidden"
    )
    assert to_bytes(svc["shm_size"]) > DEFAULT_SHM
