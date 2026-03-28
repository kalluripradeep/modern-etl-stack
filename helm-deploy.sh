#!/usr/bin/env bash
# helm-deploy.sh — Deploy the full ETL stack to Kubernetes via Helm
#
# Usage:
#   bash helm-deploy.sh
#
# Prerequisites:
#   - kubectl configured and pointing at your cluster
#   - helm 3.x installed  (https://helm.sh/docs/intro/install/)
#
# What this script does:
#   1. Adds the Apache Airflow Helm repo (once)
#   2. Downloads all chart dependencies into helm/etl-stack/charts/ (once)
#   3. Deploys the full ETL stack with a single helm install

set -euo pipefail

RELEASE_NAME="etl"
NAMESPACE="etl"
CHART_DIR="$(cd "$(dirname "$0")/helm/etl-stack" && pwd)"

GREEN='\033[0;32m'; BLUE='\033[0;34m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── Prerequisites ─────────────────────────────────────────────────────────────
command -v kubectl >/dev/null || error "kubectl not found. Install: https://kubernetes.io/docs/tasks/tools/"
command -v helm    >/dev/null || error "helm not found. Install:    https://helm.sh/docs/intro/install/"

# ── Step 1: Add Helm repo (idempotent) ────────────────────────────────────────
info "Adding Apache Airflow Helm repo..."
helm repo add apache-airflow https://airflow.apache.org --force-update
helm repo update
ok "Helm repo ready"

# ── Step 2: Download chart dependencies (Airflow subchart) ────────────────────
info "Downloading chart dependencies into $CHART_DIR/charts/ ..."
helm dependency update "$CHART_DIR"
ok "Dependencies downloaded — no internet needed from here on"

# ── Step 3: Deploy ────────────────────────────────────────────────────────────
info "Deploying ETL stack to namespace '$NAMESPACE' ..."
helm upgrade --install "$RELEASE_NAME" "$CHART_DIR" \
  --namespace "$NAMESPACE" \
  --create-namespace \
  --timeout 15m \
  --wait

ok "ETL stack is up!"

# ── Print URLs ────────────────────────────────────────────────────────────────
NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}' 2>/dev/null \
  || kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')

echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}  All services deployed!${NC}"
echo -e "${GREEN}============================================================${NC}"
echo ""
echo "  Service          URL"
echo "  ─────────────────────────────────────────────────────"
echo "  Airflow UI       http://${NODE_IP}:30880   (admin / admin123)"
echo "  Grafana          http://${NODE_IP}:30300   (admin / admin123)"
echo "  MinIO Console    http://${NODE_IP}:30901   (minioadmin / minioadmin123)"
echo "  Spark UI         http://${NODE_IP}:30808"
echo ""
echo "  Next steps:"
echo "  1. Create MinIO buckets (bronze/silver) — see DEPLOY_GUIDE.md"
echo "  2. Register Debezium connector: bash scripts/register_debezium_connector.sh"
echo ""
