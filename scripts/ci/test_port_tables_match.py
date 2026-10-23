"""The two scripts that port-forward the cluster must agree on the ports.

scripts/test_e2e.sh and scripts/with_cluster.sh each start the same set of
forwards and export the same addresses. They are separate because their
lifecycles differ -- one runs a fixed suite, the other wraps an arbitrary
command -- but a disagreement between them is invisible until something
connects to a port nothing is listening on and reports:

    connection to server at "localhost" (127.0.0.1), port 5432 failed:
    Connection refused

which reads as a broken cluster. That exact confusion has now been reported
against three separate scripts, so it is worth a build failure instead.

Checks both directions: the forwards agree, and each exported address uses
the port actually forwarded for that service.
"""
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[2]
E2E = ROOT / "scripts" / "test_e2e.sh"
WRAP = ROOT / "scripts" / "with_cluster.sh"

# `fwd <service> <local port> <remote port>`, ignoring the function definition
# itself and any commented-out line.
FWD = re.compile(r"^\s*fwd\s+([a-z0-9-]+)\s+(\d+)\s+(\d+)\s*$", re.M)

# The exported addresses, as either a bare port or a URL carrying one.
PORT_EXPORT = re.compile(r"^export\s+([A-Z_]+)=(\d+)\s*$", re.M)
URL_EXPORT = re.compile(r'^export\s+([A-Z_]+)="?http://localhost:(\d+)"?\s*$', re.M)

# Which forward each exported address is supposed to point at.
EXPECTED_SERVICE = {
    "SOURCE_DB_PORT": "postgres-source",
    "DEST_DB_PORT": "postgres-dest",
    "KAFKA_CONNECT_URL": "kafka-connect",
    "SEAWEEDFS_ENDPOINT": "seaweedfs",
    "CLICKHOUSE_URL": "clickhouse",
    "TRINO_URL": "trino",
}


def forwards(path):
    text = path.read_text(encoding="utf-8")
    found = {m.group(1): (int(m.group(2)), int(m.group(3))) for m in FWD.finditer(text)}
    assert found, f"no fwd lines parsed from {path.name} -- has the syntax changed?"
    return found


def exports(path):
    text = path.read_text(encoding="utf-8")
    out = {m.group(1): int(m.group(2)) for m in PORT_EXPORT.finditer(text)}
    out.update({m.group(1): int(m.group(2)) for m in URL_EXPORT.finditer(text)})
    return out


def test_both_scripts_forward_the_same_ports():
    a, b = forwards(E2E), forwards(WRAP)
    assert a == b, (
        "port-forward tables differ between test_e2e.sh and with_cluster.sh.\n"
        f"  test_e2e.sh:      {sorted(a.items())}\n"
        f"  with_cluster.sh:  {sorted(b.items())}"
    )


def test_exported_addresses_match_the_forwards():
    for path in (E2E, WRAP):
        fwd, exp = forwards(path), exports(path)
        for var, service in EXPECTED_SERVICE.items():
            if var not in exp:
                continue  # that script may legitimately not export this one
            assert service in fwd, f"{path.name} exports {var} but never forwards {service}"
            local = fwd[service][0]
            assert exp[var] == local, (
                f"{path.name}: {var} points at {exp[var]} but {service} "
                f"is forwarded to {local}"
            )


def test_guard_would_notice_a_drifted_port():
    """The guard has to be able to fail, or it is decoration.

    Mutates a parsed table rather than the file: the assertion under test is
    the comparison, and rewriting a script on disk during a test is a worse
    idea than it looks.
    """
    a = dict(forwards(E2E))
    b = dict(forwards(WRAP))
    assert a == b, "fixture broken: tables already differ"
    b["postgres-source"] = (9999, 5432)
    assert a != b, "comparison cannot distinguish a changed port"


if __name__ == "__main__":
    test_both_scripts_forward_the_same_ports()
    test_exported_addresses_match_the_forwards()
    test_guard_would_notice_a_drifted_port()
    print("port tables agree")
