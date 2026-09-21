#!/usr/bin/env python3
"""Guard: ClickHouse's caches must fit inside its container.

ClickHouse sizes mark_cache_size (5 GiB) and uncompressed_cache_size (8 GiB)
for a dedicated machine. Against a 4Gi pod those defaults let resident memory
drift to the ceiling on cache alone, and the failure is not an OOM kill --
the OvercommitTracker cancels whichever query happens to be running, which is
rarely the one responsible. On the reporting cluster it picked a count over
ten thousand rows, which sent the investigation after a query that was never
the problem (#182).

The relationship being guarded spans two files, which is why it is worth a
check: the cache size lives in the XML and the memory limit lives in the
deployment, and nothing otherwise connects them.

Runs offline.
"""

import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "docker" / "clickhouse" / "config.d" / "03_memory_limits.xml"
STATEFULSET = ROOT / "k8s" / "clickhouse" / "statefulset.yaml"
COMPOSE = ROOT / "docker-compose.yml"
failures = []


def check(label, condition, detail=""):
    if condition:
        print(f"  ok    {label}")
    else:
        print(f"  FAIL  {label}{(' — ' + detail) if detail else ''}")
        failures.append(label)


def to_bytes(text):
    """ClickHouse takes a plain byte count in these elements."""
    return int(str(text).strip())


check("the memory config exists", CONFIG.exists(), str(CONFIG))
if not CONFIG.exists():
    sys.exit(1)

root = ET.parse(CONFIG).getroot()
caches = {el.tag: to_bytes(el.text) for el in root if el.text and el.text.strip().isdigit()}

check(
    "mark_cache_size is declared",
    "mark_cache_size" in caches,
    "left at the default it is 5 GiB, larger than the whole container",
)

# The container limit, read from the deployment rather than restated here, so
# that raising one without the other is what fails.
sts = STATEFULSET.read_text(encoding="utf-8")
m = re.search(r"limits:\s*\n(?:\s+\w[\w-]*:\s*\"?[^\n\"]+\"?\n)*?\s+memory:\s*\"?(\d+)(Mi|Gi)\"?", sts)
check("the statefulset declares a memory limit", m is not None)

if m:
    limit = int(m.group(1)) * (1024**2 if m.group(2) == "Mi" else 1024**3)
    mark = caches.get("mark_cache_size", 5 * 1024**3)
    print(f"        container limit {limit / 1024**3:.2f} GiB, "
          f"mark cache {mark / 1024**2:.0f} MiB")
    # ClickHouse derives max_server_memory_usage at 90% of the cgroup, and the
    # cache is only one of the things competing for it. A cache allowed even
    # half the container leaves nothing for merges, Kafka consumers and the
    # queries themselves.
    check(
        "the mark cache is a fraction of the container, not most of it",
        mark <= limit // 4,
        f"{mark / 1024**2:.0f} MiB against a {limit / 1024**3:.2f} GiB limit",
    )

check(
    "the uncompressed cache is bounded",
    caches.get("uncompressed_cache_size", 8 * 1024**3) <= 256 * 1024**2,
    "nothing here sets use_uncompressed_cache, so reserving 8 GiB for it is "
    "pure overhead",
)

# Mounted in both deployments, or one of them silently keeps the defaults.
check(
    "kubernetes mounts it",
    "03_memory_limits.xml" in sts,
    "the configmap is built from the whole directory, but the subPath mount "
    "is per file",
)
check(
    "compose mounts it",
    "03_memory_limits.xml" in COMPOSE.read_text(encoding="utf-8"),
)

print()
if failures:
    print(f"{len(failures)} check(s) failed")
    sys.exit(1)
print("All ClickHouse memory guards passed")
