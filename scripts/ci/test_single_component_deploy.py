"""The committed manifests are templates, and only deploy.sh renders them.

k8s/deploy.sh rewrites storageClassName, the registry, and the Airflow secret
key into a temp copy before applying anything. So a cluster it built holds a
StatefulSet whose volumeClaimTemplates no longer matches the file in git, and
applying that file directly fails:

    The StatefulSet "postgres-source" is invalid: spec: Forbidden: updates to
    statefulset spec for fields other than 'replicas', ... are forbidden

because volumeClaimTemplates is immutable. The error lists every field you may
change and not the one that differs, so it reads as a broken manifest rather
than a rendering step that was skipped. Reproduced against a live API server
before this guard was written, and the fix verified the same way.

Until `bash k8s/deploy.sh <component>` existed, the only way to push one
component's change was a hand-written kubectl patch. A max_connections fix sat
unapplied on a cluster for four days that way while the benchmark needing it
failed identically every run, and two of the patches written around it were
wrong (#188).

What this guards is the thing that would quietly undo it: a rewrite added to
the full deploy and not to the component path. Both call render_manifests, so
that cannot happen while every rewrite lives inside it -- which is what these
assertions check. They are structural on purpose. A test that rendered and
compared trees would prove today's output matches and still pass the day
someone adds a fourth rewrite below the function.
"""
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "k8s" / "deploy.sh"
LADDER = ROOT / "scripts" / "bench" / "ladder.sh"

# Every substitution the full deploy makes on its temp copy. Each must live
# inside render_manifests, or the component path silently does not do it.
REWRITES = (
    "storageClassName: ",
    "YOUR_REGISTRY/airflow-etl",
    "image: data-dashboard:latest",
    "webserverSecretKey: ",
    "class: ",
)


def text():
    return DEPLOY.read_text(encoding="utf-8")


def render_body():
    """The body of render_manifests(), by brace matching at column 0."""
    lines = text().splitlines()
    start = next(
        i for i, line in enumerate(lines)
        if line.startswith("render_manifests() {")
    )
    end = next(i for i, line in enumerate(lines[start:], start) if line == "}")
    return "\n".join(lines[start + 1:end]), start, end


def test_the_renderer_is_a_function():
    assert "render_manifests() {" in text(), (
        "render_manifests is gone. The component path and the full deploy "
        "share it so that one cannot drift from the other; inlining it again "
        "puts the rewrites back in one path only"
    )


def test_every_rewrite_lives_inside_the_renderer():
    body, start, end = render_body()
    lines = text().splitlines()
    for rewrite in REWRITES:
        outside = [
            (i + 1, line) for i, line in enumerate(lines)
            if rewrite in line
            and not (start < i < end)
            and not line.strip().startswith("#")
            and "$TMP_K8S" in line
        ]
        assert not outside, (
            f"deploy.sh rewrites {rewrite!r} on the temp copy outside "
            f"render_manifests, at line(s) {[n for n, _ in outside]}. The "
            "component path only runs what is inside that function, so this "
            "substitution would apply to a full deploy and not to "
            "`bash k8s/deploy.sh <component>` -- the two then render "
            "differently and the component apply fails on an immutable field"
        )
        assert rewrite in body, (
            f"{rewrite!r} is no longer rewritten inside render_manifests"
        )


def test_both_paths_call_the_renderer():
    calls = re.findall(r"^\s*render_manifests\s*$", text(), re.M)
    assert len(calls) >= 2, (
        f"render_manifests is called {len(calls)} time(s); the full deploy "
        "and the single-component path should each call it. One call means a "
        "path is rendering some other way"
    )


def test_the_component_path_exits_before_the_full_deploy():
    """Otherwise `deploy.sh postgres-source` redeploys the whole stack."""
    body = text()
    branch = body.index('COMPONENT="${1:-}"')
    assert "exit 0" in body[branch:branch + 3000], (
        "the single-component branch does not exit, so it would fall through "
        "into the full deploy -- prompting for a registry and reapplying "
        "every component the caller did not ask for"
    )


def test_the_component_path_does_not_hardcode_a_storage_class():
    """It has to match what the cluster already uses, or the apply fails.

    volumeClaimTemplates is immutable, so rendering a different class than the
    live one reproduces the exact error this path exists to remove.
    """
    body = text()
    branch = body.index('COMPONENT="${1:-}"')
    section = body[branch:body.index("render_manifests", branch)]
    assert "kubectl get pvc" in section, (
        "the component path no longer reads the StorageClass the cluster is "
        "actually using. Re-detecting the default picks a different class "
        "than the operator typed, and the apply fails on volumeClaimTemplates "
        "with the error this whole path exists to avoid"
    )


def test_the_ladder_says_how_to_fix_a_stalled_mirror():
    """It reported the symptom and the diagnosis but not the remedy.

    register_debezium_connector.sh has had the one-line restart all along;
    the abort message sent people to the status endpoint and stopped there.
    """
    text_ = LADDER.read_text(encoding="utf-8")
    assert "tasks/0/restart" in text_, (
        "ladder.sh's liveness abort no longer prints the task restart. It "
        "tells you to read the connector status, which is the diagnosis; "
        "without the restart the reader has to go and find it"
    )
    assert "SECOND" in text_ or "second" in text_, (
        "the abort no longer says which 'state' to read. The connector's is "
        "first and is almost always RUNNING -- that is exactly how a dead "
        "task gets read as a healthy pipeline"
    )


if __name__ == "__main__":
    test_the_renderer_is_a_function()
    test_every_rewrite_lives_inside_the_renderer()
    test_both_paths_call_the_renderer()
    test_the_component_path_exits_before_the_full_deploy()
    test_the_component_path_does_not_hardcode_a_storage_class()
    test_the_ladder_says_how_to_fix_a_stalled_mirror()
    print("single component deploy: all cases pass")
