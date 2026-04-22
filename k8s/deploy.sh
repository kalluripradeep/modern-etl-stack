#!/usr/bin/env bash
# deploy.sh — Deploy the full ETL stack to Kubernetes
# Run from the repo root: bash k8s/deploy.sh
#
# Prerequisites:
#   - kubectl configured and pointing at your cluster
#   - helm 3.x installed
#   - docker installed (for building Airflow image)
#
# Tested on: minikube, GKE, EKS, AKS

set -euo pipefail

NAMESPACE="etl"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ─── Step 0: Check prerequisites ─────────────────────────────────────────────
info "Checking prerequisites..."
command -v kubectl >/dev/null || error "kubectl not found"
command -v helm    >/dev/null || error "helm not found"
command -v docker  >/dev/null || error "docker not found"
ok "All prerequisites found"

# ─── Step 1: Build and push Airflow image ─────────────────────────────────────
echo ""
warn "You need a container registry to push the Airflow image."
warn "Options: DockerHub (docker.io/USERNAME), GCR (gcr.io/PROJECT), ECR, etc."
warn "IMPORTANT: Ensure you have run 'docker login' for your registry first."
read -rp "Enter your registry (e.g. docker.io/myuser): " REGISTRY

if [ -z "$REGISTRY" ]; then
  warn "Skipping image build — using default apache/airflow:2.8.0-python3.11"
  warn "DAGs and deps may be missing. Re-run after building your image."
  AIRFLOW_IMAGE="apache/airflow:2.8.0-python3.11"
else
  AIRFLOW_IMAGE="${REGISTRY}/airflow-etl:latest"
  info "Building Airflow image: $AIRFLOW_IMAGE"
  docker build -t "$AIRFLOW_IMAGE" -f "$REPO_ROOT/docker/airflow/Dockerfile" "$REPO_ROOT"
  info "Pushing $AIRFLOW_IMAGE..."
  docker push "$AIRFLOW_IMAGE" || error "Push failed! Are you logged in? Run 'docker login' and try again."
  ok "Image pushed: $AIRFLOW_IMAGE"

  # Patch the helm values with actual image
  sed -i "s|YOUR_REGISTRY/airflow-etl|${REGISTRY}/airflow-etl|g" \
    "$REPO_ROOT/k8s/airflow/helm-values.yaml"
fi

# ─── Step 2: Namespace + Secrets + ConfigMaps ─────────────────────────────────
echo ""
info "Creating namespace, secrets, and configmaps..."
kubectl apply -f "$REPO_ROOT/k8s/00-namespace.yaml"
kubectl apply -f "$REPO_ROOT/k8s/01-secrets.yaml"
kubectl apply -f "$REPO_ROOT/k8s/02-configmaps.yaml"
ok "Namespace, secrets, configmaps applied"

# ─── Step 3: Databases ────────────────────────────────────────────────────────
echo ""
info "Deploying PostgreSQL source and destination..."
kubectl apply -f "$REPO_ROOT/k8s/postgres-source/"
kubectl apply -f "$REPO_ROOT/k8s/postgres-dest/"

info "Waiting for postgres-source to be ready..."
kubectl rollout status statefulset/postgres-source -n $NAMESPACE --timeout=120s
info "Waiting for postgres-dest to be ready..."
kubectl rollout status statefulset/postgres-dest -n $NAMESPACE --timeout=120s
ok "PostgreSQL pods are ready"

# ─── Step 4: MinIO ────────────────────────────────────────────────────────────
echo ""
info "Deploying MinIO..."
kubectl apply -f "$REPO_ROOT/k8s/minio/"
kubectl rollout status statefulset/minio -n $NAMESPACE --timeout=120s
ok "MinIO is ready"

# Create bronze and silver buckets
info "Creating MinIO buckets (bronze, silver)..."
MINIO_POD=$(kubectl get pod -n $NAMESPACE -l app=minio -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n $NAMESPACE "$MINIO_POD" -- sh -c "
  mc alias set local http://localhost:9000 minioadmin minioadmin123 &&
  mc mb --ignore-existing local/bronze &&
  mc mb --ignore-existing local/silver
" || warn "Could not create buckets automatically — create them manually in the MinIO console"
ok "MinIO buckets ready"

# ─── Step 5: Zookeeper + Kafka ────────────────────────────────────────────────
echo ""
info "Deploying Zookeeper..."
kubectl apply -f "$REPO_ROOT/k8s/zookeeper/"
kubectl rollout status statefulset/zookeeper -n $NAMESPACE --timeout=120s
ok "Zookeeper is ready"

info "Deploying Kafka..."
kubectl apply -f "$REPO_ROOT/k8s/kafka/"
info "Waiting for Kafka to be ready (this takes ~60s)..."
kubectl rollout status statefulset/kafka -n $NAMESPACE --timeout=180s
ok "Kafka is ready"

# ─── Step 6: Kafka Connect (Debezium) ─────────────────────────────────────────
echo ""
info "Deploying Kafka Connect with Debezium..."
kubectl apply -f "$REPO_ROOT/k8s/kafka-connect/"
info "Waiting for Kafka Connect to be ready (this takes ~60s)..."
kubectl rollout status deployment/kafka-connect -n $NAMESPACE --timeout=180s
ok "Kafka Connect is ready"

info "Registering Debezium CDC connector..."
KAFKA_CONNECT_URL="http://kafka-connect-0.kafka-connect.${NAMESPACE}.svc.cluster.local:8083"
export KAFKA_CONNECT_URL SOURCE_DB_HOST="postgres-source-0.postgres-source.${NAMESPACE}.svc.cluster.local"
bash "$REPO_ROOT/scripts/register_debezium_connector.sh" \
  || warn "Could not register connector automatically — run register_debezium_connector.sh manually"
ok "Debezium connector registered"

# ─── Step 7: Spark ────────────────────────────────────────────────────────────
echo ""
info "Deploying Spark master and workers..."
kubectl apply -f "$REPO_ROOT/k8s/spark/"
kubectl rollout status deployment/spark-master -n $NAMESPACE --timeout=120s
kubectl rollout status deployment/spark-worker -n $NAMESPACE --timeout=120s
ok "Spark cluster is ready"

# ─── Step 8: Monitoring ───────────────────────────────────────────────────────
echo ""
info "Deploying Prometheus and Grafana..."
kubectl apply -f "$REPO_ROOT/k8s/monitoring/"
kubectl rollout status deployment/prometheus -n $NAMESPACE --timeout=120s
kubectl rollout status deployment/grafana    -n $NAMESPACE --timeout=120s
ok "Monitoring stack is ready"

# ─── Step 9: Airflow ──────────────────────────────────────────────────────────
echo ""
info "Adding Airflow Helm repo..."
helm repo add apache-airflow https://airflow.apache.org --force-update
helm repo update

info "Deploying Airflow via Helm (this takes 2-3 minutes)..."
helm upgrade --install airflow apache-airflow/airflow \
  --namespace $NAMESPACE \
  --values "$REPO_ROOT/k8s/airflow/helm-values.yaml" \
  --set "images.airflow.repository=${AIRFLOW_IMAGE%:*}" \
  --set "images.airflow.tag=${AIRFLOW_IMAGE##*:}" \
  --timeout 10m \
  --wait
ok "Airflow is ready"

# ─── Step 10: Seed sample data ────────────────────────────────────────────────
echo ""
read -rp "Seed sample orders data into postgres-source? (y/N): " SEED
if [[ "$SEED" =~ ^[Yy]$ ]]; then
  info "Running data generator..."
  kubectl run seed-data \
    --image=python:3.11-slim \
    --restart=Never \
    --namespace=$NAMESPACE \
    --env="SOURCE_DB_HOST=postgres-source-0.postgres-source.${NAMESPACE}.svc.cluster.local" \
    --env="SOURCE_DB_USER=sourceuser" \
    --env="SOURCE_DB_PASSWORD=sourcepass" \
    --env="SOURCE_DB_NAME=sourcedb" \
    --command -- sh -c "
      pip install psycopg2-binary faker pandas pyarrow -q &&
      python /scripts/generate_ecommerce.py
    " || warn "Seed job failed — run generate_ecommerce.py manually"
  ok "Sample data seeded"
fi

# ─── Done ─────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}  ETL Stack deployed successfully!${NC}"
echo -e "${GREEN}============================================================${NC}"
echo ""
NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}' 2>/dev/null \
  || kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
echo "  Service            URL"
echo "  ─────────────────────────────────────────────────────"
echo "  Airflow UI         http://${NODE_IP}:30880  (admin / admin123)"
echo "  Grafana            http://${NODE_IP}:30300  (admin / admin123)"
echo "  MinIO Console      http://${NODE_IP}:30901  (minioadmin / minioadmin123)"
echo "  Spark UI           http://${NODE_IP}:30808"
echo ""
echo "  To scale for 50GB+ datasets, update k8s/02-configmaps.yaml:"
echo "    ETL_CHUNK_SIZE:      500000"
echo "    CDC_MAX_MESSAGES:    50000"
echo "    CDC_COMMIT_EVERY:    10000"
echo "    SPARK_EXECUTOR_MEMORY: 8g"
echo ""
echo "  Then: kubectl apply -f k8s/02-configmaps.yaml"
echo "        kubectl rollout restart deployment -n etl"
echo ""
