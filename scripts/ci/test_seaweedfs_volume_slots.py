"""-volume.max=0 means "free disk / volume size", not "unlimited".

SeaweedFS caps how many volumes a node may create. Run out and every write
returns HTTP 500, which boto3 reports as

    ClientError: An error occurred (InternalError) when calling the
    PutObject operation (reached max retries: 4)

and s3a as AWSStatus500Exception -- both reading like a server fault rather
than a quota.

This was set to 0 once, to escape the default of 8. `weed server -h` is
explicit that zero "will be auto configured as free disk space divided by
volume size", and volume size defaults to 30GB, so a 52GB node got 2 slots:
fewer than the 8 it was meant to fix. Each S3 bucket is its own collection
needing its own volumes and SeaweedFS grows them seven at a time, so two
slots served no bucket at all -- bronze, silver and airflow-logs were all
unwritable for a week (#192).

The number depends on the host's free disk, which is why it never showed up
on a developer laptop with room to spare. A guard is worth more than usual
here precisely because the failure cannot be reproduced on the machine where
the change gets made.
"""
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[2]
FILES = [
    ROOT / "k8s" / "seaweedfs" / "statefulset.yaml",
    ROOT / "docker-compose.yml",
]
DEPLOY = ROOT / "k8s" / "deploy.sh"
SMOKE = ROOT / ".github" / "workflows" / "deploy-smoke.yml"

# The slot count must cover one collection per bucket, grown seven at a time.
# bronze, silver, airflow-logs and the Iceberg warehouse is 28 before any data
# arrives, so anything in single digits is the old bug wearing a number.
MIN_SLOTS = 32


def weed_invocations(text):
    """The exec line only -- prose about `weed server -h` is not a config."""
    lines = []
    for ln in text.splitlines():
        s = ln.strip()
        if s.startswith("#") or "weed server" not in s:
            continue
        lines.append(s)
    return lines


def test_every_weed_server_sets_volume_max():
    for path in FILES:
        for line in weed_invocations(path.read_text(encoding="utf-8")):
            assert "-volume.max=" in line, (
                f"{path.name}: weed server runs without -volume.max, so it "
                f"falls back to a default that has already broken this twice"
            )


def test_volume_max_is_never_zero():
    """The exact regression: 0 reads as unlimited and means ~2 on a real node."""
    for path in FILES:
        for line in weed_invocations(path.read_text(encoding="utf-8")):
            m = re.search(r"-volume\.max=(\S+)", line)
            assert m, f"{path.name}: could not read -volume.max"
            assert m.group(1) != "0", (
                f"{path.name}: -volume.max=0 is back. Zero is not unlimited -- "
                f"it is free disk divided by 30GB, which was 2 slots on the "
                f"cluster in #192 and made every PutObject return InternalError"
            )


def test_slot_count_covers_one_collection_per_bucket():
    for path in FILES:
        for line in weed_invocations(path.read_text(encoding="utf-8")):
            count = int(re.search(r"-volume\.max=(\d+)", line).group(1))
            assert count >= MIN_SLOTS, (
                f"{path.name}: -volume.max={count} is below {MIN_SLOTS}. Each "
                f"bucket is a collection and SeaweedFS grows volumes seven at "
                f"a time, so this cannot serve the four buckets this stack uses"
            )


def test_the_node_disk_keeps_a_floor():
    """local-path PVCs sit on the node's root disk; 1% of it is not a margin.

    Raising the volume ceiling is what makes filling the disk reachable, so
    the floor belongs in the same change as the ceiling.
    """
    for path in FILES:
        for line in weed_invocations(path.read_text(encoding="utf-8")):
            assert "-volume.minFreeSpace=" in line, (
                f"{path.name}: no -volume.minFreeSpace. The default is 1%, "
                f"which on a 100GB node disk shared with every other pod "
                f"leaves about a gigabyte before writes stop everywhere"
            )


def test_both_deployments_agree():
    """Compose is where this gets changed; k8s is where it breaks."""
    flags = []
    for path in FILES:
        for line in weed_invocations(path.read_text(encoding="utf-8")):
            flags.append(re.findall(r"-volume\.(?:max|minFreeSpace)=\S+", line))
    assert len(flags) == len(FILES), "expected one weed server line per file"
    assert flags[0] == flags[1], (
        f"Compose and k8s disagree on the volume limits: {flags[0]} vs "
        f"{flags[1]}. They diverged silently for a week once already"
    )


def test_deploy_probes_writability_not_just_existence():
    """Listing a bucket proves nothing; it costs no volume.

    Both of these once asserted only that `s3.bucket.list` named the buckets,
    and both passed for a week against a cluster where every write returned
    500. On the same broken node the listing succeeds and a write returns 500,
    so the write is the only check that distinguishes them.
    """
    for path in (DEPLOY, SMOKE):
        text = path.read_text(encoding="utf-8")
        assert "s3.bucket.list" in text, f"{path.name}: lost the bucket listing"
        assert "/buckets/$b/" in text, (
            f"{path.name}: no write probe against the filer. Checking that a "
            f"bucket exists passes on a node that can store nothing (#192)"
        )
        assert '-X DELETE' in text, (
            f"{path.name}: the probe does not clean up after itself"
        )


def test_an_unwritable_bucket_fails_rather_than_warns():
    """A deploy leaving the lakehouse unwritable must not report success.

    The missing-bucket case was a warning for exactly this reason and it
    was not loud enough: the deploy went green, nobody read the line, and
    the cluster was unusable for a week.
    """
    text = DEPLOY.read_text(encoding="utf-8")
    branch = text[text.index('if [ -n "$UNWRITABLE" ]'):]
    branch = branch[: branch.index("fi")]
    assert "error " in branch, (
        "deploy.sh no longer treats an unwritable bucket as fatal; it would "
        "report a clean deploy of a lakehouse that cannot be written to"
    )
    assert "warn " not in branch, (
        "an unwritable bucket is only a warning again (#192)"
    )


def test_probe_has_no_line_continuations():
    """The probe runs through `sh -c`; a wrapped line is a silent breakage.

    A backslash-newline inside the probe has already been collapsed once by
    tooling between here and the file. The result still parsed and quietly
    no longer did what it said, which is the failure mode this whole guard
    exists to prevent.
    """
    for path in (DEPLOY, SMOKE):
        text = path.read_text(encoding="utf-8")
        key = "UNWRITABLE=" if path is DEPLOY else "unwritable="
        probe = text[text.index(key):]
        probe = probe[: probe.index("2>/dev/null)")]
        for line in probe.splitlines():
            assert not line.rstrip().endswith(chr(92)), (
                f"{path.name}: line continuation in the write probe: "
                f"{line.strip()!r} -- these get collapsed in transit and "
                f"the result still parses, so the probe stops probing"
            )


if __name__ == "__main__":
    test_every_weed_server_sets_volume_max()
    test_volume_max_is_never_zero()
    test_slot_count_covers_one_collection_per_bucket()
    test_the_node_disk_keeps_a_floor()
    test_both_deployments_agree()
    test_deploy_probes_writability_not_just_existence()
    test_an_unwritable_bucket_fails_rather_than_warns()
    test_probe_has_no_line_continuations()
    print("seaweedfs volume slots: all cases pass")
