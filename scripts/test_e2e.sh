#!/usr/bin/env bash
# test_e2e.sh — End-to-end ETL test against a live Kubernetes cluster
#
# What it does:
#   1. Port-forwards all K8s services to localhost
#   2. Installs Python test dependencies in a venv
#   3. Runs scripts/test_transactions.py (seed → extract → CDC → verify)
#   4. Prints a final pass/fail summary
#
# Usage:
#   bash scripts/test_e2e.sh
#
# Requirements:
#   - kubectl configured and pointed at your cluster
#   - The etl namespace fully deployed (run bash k8s/deploy.sh first)
#   - python3 and pip available

set -euo pipefail

NAMESPACE="etl"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENV="$REPO_ROOT/.venv-test"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; BLUE='\033[0;34m'; NC='\033[0m'
info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── 0. Prerequisites ──────────────────────────────────────────────────────────
info "Checking prerequisites..."
command -v kubectl  >/dev/null || error "kubectl not found"
command -v python3  >/dev/null || error "python3 not found"

# Verify the etl namespace exists and pods are up
kubectl get namespace $NAMESPACE >/dev/null 2>&1 \
  || error "Namespace '$NAMESPACE' not found — run 'bash k8s/deploy.sh' first"

NOT_READY=$(kubectl get pods -n $NAMESPACE --no-headers 2>/dev/null \
  | grep -v Running | grep -v Completed | grep -c . || true)
if [ "$NOT_READY" -gt 0 ]; then
  warn "$NOT_READY pod(s) not in Running state — proceeding anyway, some steps may fail"
  kubectl get pods -n $NAMESPACE
fi
ok "Prerequisites OK"

# ── 1. Port-forward all services ─────────────────────────────────────────────
info "Starting port-forwards (all run in background)..."

pf_pids=()

fwd() {
  local svc=$1 local_port=$2 remote_port=$3
  kubectl port-forward "svc/$svc" "${local_port}:${remote_port}" -n $NAMESPACE \
    >/tmp/pf-${svc}.log 2>&1 &
  pf_pids+=($!)
  echo "  port-forward svc/$svc  localhost:${local_port} → ${remote_port}"
}

fwd postgres-source 5433 5432
fwd postgres-dest   5434 5432
fwd kafka           9093 9092
fwd kafka-connect   8084 8083
fwd minio           9000 9000

# Give port-forwards a moment to settle
sleep 3

cleanup() {
  info "Stopping port-forwards..."
  for pid in "${pf_pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  ok "Port-forwards stopped"
}
trap cleanup EXIT

# Verify at least postgres-source is reachable
if ! nc -z localhost 5433 2>/dev/null; then
  warn "postgres-source port-forward does not seem ready yet — waiting 5s..."
  sleep 5
fi
ok "Port-forwards active"

# ── 2. Set up Python venv ─────────────────────────────────────────────────────
info "Setting up Python virtual environment..."
if [ ! -d "$VENV" ]; then
  python3 -m venv "$VENV"
fi
# shellcheck disable=SC1091
source "$VENV/bin/activate"

pip install --quiet --upgrade pip
pip install --quiet \
  psycopg2-binary \
  pandas \
  pyarrow \
  faker \
  kafka-python \
  minio \
  requests

ok "Python environment ready"

# ── 3. Run the transactional test ─────────────────────────────────────────────
info "Running transactional test suite..."
echo ""

export SOURCE_DB_HOST=localhost
export SOURCE_DB_PORT=5433
export DEST_DB_HOST=localhost
export DEST_DB_PORT=5434
export KAFKA_BOOTSTRAP="localhost:9093"
export KAFKA_CONNECT_URL="http://localhost:8084"
export MINIO_ENDPOINT="http://localhost:9000"
export SOURCE_DB_NAME=sourcedb
export SOURCE_DB_USER=sourceuser
export SOURCE_DB_PASSWORD=sourcepass
export DEST_DB_NAME=destdb
export DEST_DB_USER=destuser
export DEST_DB_PASSWORD=destpass
export MINIO_ROOT_USER=minioadmin
export MINIO_ROOT_PASSWORD=minioadmin123

set +e   # don't exit on test failure — we want the cleanup trap to run
python3 "$REPO_ROOT/scripts/test_transactions.py"
TEST_EXIT=$?
set -e

# ── 4. Final result ───────────────────────────────────────────────────────────
echo ""
if [ $TEST_EXIT -eq 0 ]; then
  echo -e "${GREEN}============================================================${NC}"
  echo -e "${GREEN}  ALL TESTS PASSED — pipeline is healthy!${NC}"
  echo -e "${GREEN}============================================================${NC}"
else
  echo -e "${RED}============================================================${NC}"
  echo -e "${RED}  SOME TESTS FAILED — check the output above${NC}"
  echo -e "${RED}============================================================${NC}"
fi

exit $TEST_EXIT
