"""Kafka has to be told how long to keep what the benchmarks write.

The default log.retention.hours is 168 -- seven days -- and neither
deployment ever set it. A ladder arm writes two messages per transaction to
cdc.public.orders (the INSERT and the UPDATE in scripts/bench/orders.sql),
so an afternoon of benchmarking at ~11,000 msg/s leaves millions of Debezium
envelopes on disk for a week.

#225 is what that costs. kafka-log0 reached 17G of a 43G local-path
directory, the node went into DiskPressure, and the kubelet rejected dozens
of pods on arrival -- they were never running, they were turned away. The
Iceberg side of the stack has had a daily expire_snapshots task since it was
written; Kafka had nothing at all.

Both deployments must carry the same bounds. A value set on one side only is
exactly the shape of the SeaweedFS volume-slot bug (#192) and the source
settings bug (#188): wrong in k8s and compose alike, unnoticed because only
one of them was ever read.

The size bound is deliberately generous. retention.bytes deletes unconsumed
messages as readily as consumed ones, and ClickHouse peaked 3.17M messages
behind during the #188 ladder -- roughly 2.1 GiB. A tighter bound would have
dropped rows the mirror had not read yet, with nothing to show it happened.
Tighten it only once Pipe 3 keeps up; this guard enforces a floor, not an
exact value.
"""
import pathlib

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]
KAFKA = ROOT / "k8s" / "kafka" / "kafka-cluster.yaml"
COMPOSE = ROOT / "docker-compose.yml"

# Below this, a normal ladder run can outrun the retention bound and delete
# messages ClickHouse has not consumed. 2.1 GiB was the measured peak lag.
MIN_RETENTION_BYTES = 2.1 * 1024**3
MAX_RETENTION_HOURS = 24


def _kafka_resource():
    for doc in yaml.safe_load_all(KAFKA.read_text(encoding="utf-8")):
        if doc and doc.get("kind") == "Kafka":
            return doc["spec"]["kafka"]["config"]
    raise AssertionError("no Kafka resource in k8s/kafka/kafka-cluster.yaml")


def test_k8s_bounds_retention_by_time_and_size():
    config = _kafka_resource()
    assert int(config["log.retention.hours"]) <= MAX_RETENTION_HOURS
    assert int(config["log.retention.bytes"]) >= MIN_RETENTION_BYTES
    # Retention only deletes closed segments, so a segment larger than the
    # bound means the bound can never be enforced.
    assert int(config["log.segment.bytes"]) < int(config["log.retention.bytes"])


def test_compose_carries_the_same_bounds():
    env = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))["services"]["kafka"]["environment"]
    config = _kafka_resource()
    assert int(env["KAFKA_LOG_RETENTION_HOURS"]) == int(config["log.retention.hours"])
    assert int(env["KAFKA_LOG_RETENTION_BYTES"]) == int(config["log.retention.bytes"])
    assert int(env["KAFKA_LOG_SEGMENT_BYTES"]) == int(config["log.segment.bytes"])
