#!/usr/bin/env bash
# with_cluster.sh — run any command against the Kubernetes deployment.
#
#   bash scripts/with_cluster.sh python3 scripts/simulate_live_traffic.py --rate 100
#   bash scripts/with_cluster.sh python3 scripts/benchmark.py
#   bash scripts/with_cluster.sh python3 scripts/test_lakehouse_interop.py
#   bash scripts/with_cluster.sh bash          # interactive shell with everything set
#
# WHY THIS EXISTS
#   test_e2e.sh sets up port-forwards and credentials, runs the suite, and then
#   tears the forwards down again through a trap on EXIT. That is correct for
#   what it does, but it makes this a contradiction:
#
#       bash scripts/test_e2e.sh      # "exports the connection settings"
#       python3 scripts/benchmark.py  # <- forwards already gone, env never set
#
#   The second command runs in a different process, so none of the exports
#   reach it, and the forwards died with the first. Everything then points at
#   localhost on compose's ports, where nothing is listening:
#
#       connection to server at "localhost" (127.0.0.1), port 5432 failed:
#       Connection refused
#
#   That reads as a broken cluster rather than a missing forward, and it has
#   now been reported three times against three different scripts.
#
#   Running the command *inside* the same process that owns the forwards is
#   the only arrangement where the two cannot drift apart.
set -euo pipefail

NAMESPACE="${NAMESPACE:-etl}"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if [ $# -eq 0 ]; then
  sed -n '3,8p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
fi

command -v kubectl >/dev/null || { echo "kubectl not found" >&2; exit 1; }
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 \
  || { echo "namespace '$NAMESPACE' not found — deploy with k8s/deploy.sh first" >&2; exit 1; }

pf_pids=()
cleanup() {
  for pid in "${pf_pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT

fwd() {
  kubectl port-forward "svc/$1" "${2}:${3}" -n "$NAMESPACE" >"/tmp/pf-$1.log" 2>&1 &
  pf_pids+=($!)
}

# Keep in step with scripts/test_e2e.sh. scripts/ci/test_port_tables_match.py
# fails the build if these two drift, because a silent mismatch here produces
# exactly the "connection refused" confusion this script exists to remove.
fwd postgres-source 5433 5432
fwd postgres-dest   5434 5432
fwd kafka-connect   8084 8083
fwd seaweedfs       9000 9000
fwd clickhouse      8123 8123
fwd trino           8085 8080

export SOURCE_DB_HOST=localhost
export SOURCE_DB_PORT=5433
export DEST_DB_HOST=localhost
export DEST_DB_PORT=5434
export KAFKA_CONNECT_URL="http://localhost:8084"
export SEAWEEDFS_ENDPOINT="http://localhost:9000"
export CLICKHOUSE_URL="http://localhost:8123"
export TRINO_URL="http://localhost:8085"

# Credentials come from the deployed secret rather than the compose defaults,
# because generate-secrets.sh randomises them. Each falls back so that a
# cluster deployed without that step still works.
sv() {
  local v
  v=$(kubectl get secret etl-secrets -n "$NAMESPACE" -o "jsonpath={.data.$1}" 2>/dev/null | base64 -d 2>/dev/null || true)
  echo "${v:-$2}"
}
SOURCE_DB_NAME=$(sv SOURCE_DB_NAME sourcedb);                          export SOURCE_DB_NAME
SOURCE_DB_USER=$(sv SOURCE_DB_USER sourceuser);                        export SOURCE_DB_USER
SOURCE_DB_PASSWORD=$(sv SOURCE_DB_PASSWORD sourcepass);                export SOURCE_DB_PASSWORD
DEST_DB_NAME=$(sv DEST_DB_NAME destdb);                                export DEST_DB_NAME
DEST_DB_USER=$(sv DEST_DB_USER destuser);                              export DEST_DB_USER
DEST_DB_PASSWORD=$(sv DEST_DB_PASSWORD destpass);                      export DEST_DB_PASSWORD
SEAWEEDFS_ROOT_USER=$(sv SEAWEEDFS_ROOT_USER seaweedadmin);            export SEAWEEDFS_ROOT_USER
SEAWEEDFS_ROOT_PASSWORD=$(sv SEAWEEDFS_ROOT_PASSWORD seaweedadmin123); export SEAWEEDFS_ROOT_PASSWORD
CLICKHOUSE_USER=$(sv CLICKHOUSE_USER chuser);                          export CLICKHOUSE_USER
CLICKHOUSE_PASSWORD=$(sv CLICKHOUSE_PASSWORD chpass123);               export CLICKHOUSE_PASSWORD

# TARGET tells the scripts that already branch on it which stack they are
# looking at, so their own defaults line up with the forwards above.
export TARGET="${TARGET:-k8s}"

# Give the forwards a moment, then confirm one is actually listening. Starting
# them in the background always "succeeds"; the failure surfaces later as a
# refused connection from whatever command runs next, which is the confusion
# this script is meant to end.
sleep 3
if ! kubectl exec -n "$NAMESPACE" postgres-source-0 -- pg_isready -q 2>/dev/null; then
  echo "warning: postgres-source-0 is not ready — the command may still fail" >&2
fi

cd "$REPO_ROOT"
set +e
"$@"
rc=$?
set -e
exit "$rc"
