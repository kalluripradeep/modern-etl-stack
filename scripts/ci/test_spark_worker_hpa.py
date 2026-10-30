"""An autoscaled Deployment must not also state how many replicas it has.

k8s/deploy.sh runs `kubectl apply -f k8s/spark/`. If spark-worker's manifest
declares spec.replicas, that value is reapplied on every deploy, so a tier the
autoscaler had grown gets reset and has to climb again. Demonstrated on a real
cluster:

    manifest pins replicas: 2    scale to 5 -> redeploy -> 2
    manifest omits replicas      scale to 5 -> redeploy -> 5 -> 5

The floor moved to the HPA's minReplicas, which is where it belongs: one
object owning the count.

The rest of these are the things that make a CPU-target HPA meaningless if
they drift -- a target that names the wrong workload, a container with no CPU
request to be a percentage of, and a scale-down window short enough to take
executors with it.
"""
import pathlib

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]
HPA = ROOT / "k8s" / "spark" / "worker-hpa.yaml"
DEPLOYMENT = ROOT / "k8s" / "spark" / "worker-deployment.yaml"

# Removing a worker kills the executors on it. Spark retries lost tasks, but
# a job that keeps losing them fails, so the window has to be long enough
# that a gap between stages does not read as idle.
MIN_SCALEDOWN_WINDOW = 300


def load(path):
    """First real document -- these files carry trailing comment blocks."""
    docs = [d for d in yaml.safe_load_all(path.read_text(encoding="utf-8")) if d]
    assert docs, f"{path.name} parsed to nothing"
    return docs[0]


def test_hpa_targets_the_worker_deployment():
    hpa = load(HPA)
    dep = load(DEPLOYMENT)
    ref = hpa["spec"]["scaleTargetRef"]
    assert ref["kind"] == dep["kind"], (
        f"the HPA targets a {ref['kind']} but spark-worker is a {dep['kind']}"
    )
    assert ref["name"] == dep["metadata"]["name"], (
        f"the HPA targets {ref['name']!r}, not {dep['metadata']['name']!r}; an "
        "HPA whose target does not resolve reports AbleToScale=False and "
        "silently does nothing"
    )
    assert hpa["metadata"]["namespace"] == dep["metadata"]["namespace"], (
        "an HPA only scales a target in its own namespace"
    )


def test_the_autoscaled_deployment_does_not_pin_replicas():
    dep = load(DEPLOYMENT)
    assert "replicas" not in dep["spec"], (
        "spark-worker declares spec.replicas while an HPA manages it. "
        "deploy.sh reapplies this file, so every deploy would reset a scaled "
        "tier back to the pinned value. The floor belongs in the HPA's "
        "minReplicas instead."
    )


def test_cpu_target_has_a_request_to_be_a_percentage_of():
    """averageUtilization is a percentage of the request, not the limit."""
    hpa = load(HPA)
    cpu_targets = [
        m for m in hpa["spec"]["metrics"]
        if m.get("resource", {}).get("name") == "cpu"
    ]
    if not cpu_targets:
        return
    containers = load(DEPLOYMENT)["spec"]["template"]["spec"]["containers"]
    for c in containers:
        request = c.get("resources", {}).get("requests", {}).get("cpu")
        assert request, (
            f"container {c['name']!r} has no CPU request, so a CPU "
            "utilization target has no denominator and the HPA cannot "
            "compute a replica count at all"
        )


def test_bounds_leave_room_to_scale():
    spec = load(HPA)["spec"]
    lo, hi = spec["minReplicas"], spec["maxReplicas"]
    assert lo >= 2, (
        f"minReplicas is {lo}; spark-worker carried 2 before the HPA existed "
        "and the floor was moved here, not lowered"
    )
    assert hi > lo, f"maxReplicas {hi} is not above minReplicas {lo}"


def test_scaledown_is_slow_enough_not_to_take_executors_with_it():
    behavior = load(HPA)["spec"].get("behavior", {})
    down = behavior.get("scaleDown", {})
    window = down.get("stabilizationWindowSeconds")
    assert window is not None, (
        "scaleDown.stabilizationWindowSeconds is unset. Removing a worker "
        "kills the executors running on it, so this is the one field the "
        "whole risk of this object lives in -- state it rather than inherit it"
    )
    assert window >= MIN_SCALEDOWN_WINDOW, (
        f"scaleDown stabilization is {window}s, under {MIN_SCALEDOWN_WINDOW}s. "
        "A gap between Spark stages would read as idle and start evicting "
        "workers that are about to be used again"
    )


if __name__ == "__main__":
    test_hpa_targets_the_worker_deployment()
    test_the_autoscaled_deployment_does_not_pin_replicas()
    test_cpu_target_has_a_request_to_be_a_percentage_of()
    test_bounds_leave_room_to_scale()
    test_scaledown_is_slow_enough_not_to_take_executors_with_it()
    print("spark worker hpa: all cases pass")
