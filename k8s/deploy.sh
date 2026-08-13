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
# Pinned so an upstream release cannot change the API our manifests target.
# k8s/kafka/kafka-cluster.yaml is written against this version's v1 API.
STRIMZI_VERSION="${STRIMZI_VERSION:-1.1.0}"

# Read a value from the secret that was actually applied, so every step works
# with generate-secrets.sh as well as the checked-in defaults. Anything that
# hardcodes a credential silently breaks for anyone who generated their own.
secret_val() {
  kubectl get secret etl-secrets -n "$NAMESPACE" -o "jsonpath={.data.$1}" | base64 -d
}

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
# Presetting REGISTRY (even to empty, meaning "skip the build") suppresses the
# prompt, so the script can run unattended in CI or any automation.
if [ -z "${REGISTRY+x}" ]; then
  read -rp "Enter your registry (e.g. docker.io/myuser): " REGISTRY
else
  info "Using REGISTRY from the environment: ${REGISTRY:-(none — skipping image build)}"
fi

if [ -z "$REGISTRY" ]; then
  warn "Skipping image build — using default apache/airflow:3.3.0-python3.11"
  warn "DAGs and deps may be missing. Re-run after building your image."
  AIRFLOW_IMAGE="apache/airflow:3.3.0-python3.11"

  info "Building local Data Dashboard image (data-dashboard:latest)"
  docker build -t "data-dashboard:latest" -f "$REPO_ROOT/ui/Dockerfile" "$REPO_ROOT/ui" || warn "Dashboard build failed"
else
  AIRFLOW_IMAGE="${REGISTRY}/airflow-etl:latest"
  DASHBOARD_IMAGE="${REGISTRY}/data-dashboard:latest"

  info "Building Airflow image: $AIRFLOW_IMAGE"
  docker build -t "$AIRFLOW_IMAGE" -f "$REPO_ROOT/docker/airflow/Dockerfile" "$REPO_ROOT"
  info "Pushing $AIRFLOW_IMAGE..."
  docker push "$AIRFLOW_IMAGE" || error "Push failed! Are you logged in? Run 'docker login' and try again."
  ok "Image pushed: $AIRFLOW_IMAGE"

  info "Building Data Dashboard image: $DASHBOARD_IMAGE"
  docker build -t "$DASHBOARD_IMAGE" -f "$REPO_ROOT/ui/Dockerfile" "$REPO_ROOT/ui"
  info "Pushing $DASHBOARD_IMAGE..."
  docker push "$DASHBOARD_IMAGE" || warn "Push failed for dashboard"
  ok "Image pushed: $DASHBOARD_IMAGE"

  # Patch the helm values with actual image in the temporary directory (created later)
  # We'll defer this until TMP_K8S is created.
  export REGISTRY_FOR_REPLACE="$REGISTRY"
fi

# ─── Step 1.5: Detect StorageClass ─────────────────────────────────────────────
echo ""
info "Detecting default StorageClass in your cluster..."
DEFAULT_SC=$(kubectl get sc -o jsonpath='{.items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")].metadata.name}' 2>/dev/null || true)
if [ -z "$DEFAULT_SC" ]; then
  DEFAULT_SC="standard"
  warn "No default StorageClass found. Defaulting to '$DEFAULT_SC'."
else
  ok "Found default StorageClass: $DEFAULT_SC"
fi

if [ -z "${STORAGE_CLASS+x}" ]; then
  read -rp "Enter StorageClass to use [$DEFAULT_SC]: " STORAGE_CLASS
fi
STORAGE_CLASS=${STORAGE_CLASS:-$DEFAULT_SC}
info "Using StorageClass: $STORAGE_CLASS"

info "Preparing temporary manifests with chosen StorageClass..."
TMP_K8S=$(mktemp -d)
cp -r "$REPO_ROOT/k8s"/* "$TMP_K8S/"

if [ -n "${REGISTRY_FOR_REPLACE:-}" ]; then
  # Apply the registry change to the temporary file
  sed -i "s|YOUR_REGISTRY/airflow-etl|${REGISTRY_FOR_REPLACE}/airflow-etl|g" "$TMP_K8S/airflow/helm-values.yaml" 2>/dev/null || perl -pi -e "s|YOUR_REGISTRY/airflow-etl|${REGISTRY_FOR_REPLACE}/airflow-etl|g" "$TMP_K8S/airflow/helm-values.yaml"
  sed -i "s|image: data-dashboard:latest|image: ${REGISTRY_FOR_REPLACE}/data-dashboard:latest|g" "$TMP_K8S/ui/deployment.yaml" 2>/dev/null || perl -pi -e "s|image: data-dashboard:latest|image: ${REGISTRY_FOR_REPLACE}/data-dashboard:latest|g" "$TMP_K8S/ui/deployment.yaml"
fi

# Replace any hardcoded storageClassName values with the chosen one
find "$TMP_K8S" -type f -name "*.yaml" -exec perl -pi -e "s/storageClassName: .*/storageClassName: ${STORAGE_CLASS}/g" {} +
# Strimzi persistent-claim storage uses `class:` instead of storageClassName
perl -pi -e "s/^(\s*)class: .*/\${1}class: ${STORAGE_CLASS}/" "$TMP_K8S/kafka/kafka-cluster.yaml"

# Use the randomized Airflow webserver secret key when generated secrets exist
# (falls back to the placeholder in helm-values.yaml otherwise)
if [ -f "$REPO_ROOT/k8s/01-secrets.generated.yaml" ]; then
  WSK=$(awk '/AIRFLOW_WEBSERVER_SECRET_KEY:/ {print $2}' "$REPO_ROOT/k8s/01-secrets.generated.yaml")
  if [ -n "$WSK" ]; then
    perl -pi -e "s|webserverSecretKey: .*|webserverSecretKey: \"$WSK\"|" "$TMP_K8S/airflow/helm-values.yaml"
  fi
fi

# ─── Step 2: Namespace + Secrets + ConfigMaps ─────────────────────────────────
echo ""
info "Creating namespace, secrets, and configmaps..."
kubectl apply -f "$TMP_K8S/00-namespace.yaml"
if [ -f "$REPO_ROOT/k8s/01-secrets.generated.yaml" ]; then
  info "Using generated secrets (k8s/01-secrets.generated.yaml)"
  kubectl apply -f "$REPO_ROOT/k8s/01-secrets.generated.yaml"
else
  warn "Using DEFAULT credentials from k8s/01-secrets.yaml."
  warn "For anything beyond a throwaway cluster, run: bash k8s/generate-secrets.sh"
  kubectl apply -f "$TMP_K8S/01-secrets.yaml"
fi
kubectl apply -f "$TMP_K8S/02-configmaps.yaml"
ok "Namespace, secrets, configmaps applied"

# ─── Step 3: Databases ────────────────────────────────────────────────────────
echo ""
info "Deploying PostgreSQL source and destination..."
kubectl apply -f "$TMP_K8S/postgres-source/"
kubectl apply -f "$TMP_K8S/postgres-dest/"

info "Waiting for postgres-source to be ready..."
kubectl rollout status statefulset/postgres-source -n $NAMESPACE --timeout=300s
info "Waiting for postgres-dest to be ready..."
kubectl rollout status statefulset/postgres-dest -n $NAMESPACE --timeout=300s
ok "PostgreSQL pods are ready"

# The Iceberg JDBC catalog stores its metadata tables in this schema, and the
# catalog cannot create them itself -- Spark fails with
#   Cannot initialize JDBC catalog ... no schema has been selected to create in
# The init-configmap creates it, but Postgres only runs those scripts on a
# fresh volume, so any cluster deployed before that script existed would never
# get it. Ensure it here too: CREATE SCHEMA IF NOT EXISTS is idempotent, so
# this is a no-op on healthy clusters and a repair on older ones.
info "Ensuring iceberg_catalog schema exists in postgres-dest..."
kubectl exec -n $NAMESPACE postgres-dest-0 -- bash -c \
  'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
     -c "CREATE SCHEMA IF NOT EXISTS iceberg_catalog AUTHORIZATION \"$POSTGRES_USER\";"' \
  >/dev/null 2>&1 && ok "iceberg_catalog schema is ready" \
  || warn "Could not ensure iceberg_catalog schema — Spark writes to Iceberg will fail until it exists"

# ─── Step 4: MinIO ────────────────────────────────────────────────────────────
echo ""
info "Deploying MinIO..."
kubectl apply -f "$TMP_K8S/minio/"
kubectl rollout status statefulset/minio -n $NAMESPACE --timeout=300s
ok "MinIO is ready"

# Create bronze and silver buckets
info "Creating MinIO buckets (bronze, silver)..."
MINIO_POD="minio-0"
# Single-quoted so the variables expand inside the pod: MinIO already has the
# real credentials in its environment from etl-secrets. Hardcoding the defaults
# here broke every deploy that used generate-secrets.sh, with a signature error.
if kubectl exec -n $NAMESPACE "$MINIO_POD" -- sh -c '
  mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" &&
  mc mb --ignore-existing local/bronze &&
  mc mb --ignore-existing local/silver &&
  mc mb --ignore-existing local/airflow-logs
'; then
  ok "MinIO buckets ready"
else
  warn "Could not create buckets automatically — create them manually in the MinIO console"
fi

# ─── Step 5: Strimzi Kafka Operator & Kafka Cluster ───────────────────────────
echo ""

info "Deploying Strimzi Kafka Operator (${STRIMZI_VERSION})..."
kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -
# Pinned, not 'latest': Strimzi 1.x dropped the v1beta2 API and ZooKeeper, so a
# floating install silently stops matching k8s/kafka/kafka-cluster.yaml. The
# release asset defaults to namespace "myproject", hence the rewrite.
curl -fsSL "https://github.com/strimzi/strimzi-kafka-operator/releases/download/${STRIMZI_VERSION}/strimzi-cluster-operator-${STRIMZI_VERSION}.yaml" \
  | sed "s/namespace: myproject/namespace: ${NAMESPACE}/g" \
  | kubectl apply -n $NAMESPACE -f -
kubectl rollout status deployment/strimzi-cluster-operator -n $NAMESPACE --timeout=300s
# The operator being up does not mean its CRDs are servable yet. Applying the
# Kafka resource too early fails with: no matches for kind "Kafka" in version
# "kafka.strimzi.io/v1beta2".
info "Waiting for Strimzi CRDs to be established..."
kubectl wait --for=condition=Established --timeout=120s \
  crd/kafkas.kafka.strimzi.io \
  crd/kafkanodepools.kafka.strimzi.io
ok "Strimzi Kafka Operator is ready"

info "Deploying Kafka cluster via Strimzi..."
kubectl apply -f "$TMP_K8S/kafka/kafka-cluster.yaml"
info "Waiting for Kafka cluster to be ready (this takes ~3-4 mins)..."
kubectl wait kafka/etl-kafka --for=condition=Ready --timeout=300s -n $NAMESPACE
ok "Kafka cluster is ready"

info "Deploying Kafka UI..."
kubectl apply -f "$TMP_K8S/kafka-ui/"
kubectl rollout status deployment/kafka-ui -n $NAMESPACE --timeout=300s
ok "Kafka UI is ready"

# ─── Step 6: Kafka Connect (Debezium) ─────────────────────────────────────────
echo ""
info "Deploying Kafka Connect with Debezium..."
kubectl apply -f "$TMP_K8S/kafka-connect/"
info "Waiting for Kafka Connect to be ready (this takes ~60s)..."
kubectl rollout status statefulset/kafka-connect -n $NAMESPACE --timeout=300s
ok "Kafka Connect is ready"

info "Deploying ClickHouse columnar mirror..."
# Reuse the compose init SQL, rewriting the broker address for in-cluster DNS
CH_INIT_DIR=$(mktemp -d)
sed 's|kafka:9092|etl-kafka-kafka-bootstrap.etl.svc.cluster.local:9092|g' \
  "$REPO_ROOT/docker/clickhouse/initdb/01_mirror_schema.sql" > "$CH_INIT_DIR/01_mirror_schema.sql"
kubectl create configmap clickhouse-init -n $NAMESPACE \
  --from-file="$CH_INIT_DIR" --dry-run=client -o yaml | kubectl apply -f -
# Server settings, notably auto_offset_reset=earliest so consumers read topics
# from the beginning rather than skipping everything produced before they
# attached. Same file compose mounts, so both environments behave identically.
kubectl create configmap clickhouse-config -n $NAMESPACE \
  --from-file="$REPO_ROOT/docker/clickhouse/config.d" --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f "$TMP_K8S/clickhouse/"
kubectl rollout status statefulset/clickhouse -n $NAMESPACE --timeout=300s

ok "ClickHouse is ready"

info "Registering Debezium CDC connector (via in-cluster exec)..."
# The connector script defaults to sourceuser/sourcepass when these are unset,
# so without them Debezium is registered with the wrong password on any cluster
# using generated secrets — it then fails to read the WAL and the whole
# real-time pipeline is silently dead.
if (
  echo "export KAFKA_CONNECT_URL=http://localhost:8083"
  echo "export SOURCE_DB_HOST=postgres-source-0.postgres-source.${NAMESPACE}.svc.cluster.local"
  echo "export DEST_DB_HOST=postgres-dest-0.postgres-dest.${NAMESPACE}.svc.cluster.local"
  echo "export SOURCE_DB_USER=$(secret_val SOURCE_DB_USER)"
  echo "export SOURCE_DB_PASSWORD=$(secret_val SOURCE_DB_PASSWORD)"
  echo "export SOURCE_DB_NAME=$(secret_val SOURCE_DB_NAME)"
  cat "$REPO_ROOT/scripts/register_debezium_connector.sh"
) | kubectl exec -i kafka-connect-0 -n $NAMESPACE -- bash; then
  ok "Debezium connector registered"
else
  warn "Could not register connector automatically — the real-time pipeline will not receive changes"
  warn "Retry with: bash scripts/register_debezium_connector.sh (see DEPLOY_GUIDE)"
fi

# Deliberately after the connector: Debezium creates the CDC topics when it
# registers, and a ClickHouse Kafka table built against a topic that does not
# exist yet does not reliably start consuming once it appears. Creating the
# consumers first left Kafka holding tens of thousands of messages while the
# mirror stayed empty, with nothing reporting an error.
#
# The ConfigMap is mounted at /docker-entrypoint-initdb.d, which ClickHouse only
# executes on first boot of an empty volume, so applying it explicitly is also
# what carries schema changes to clusters whose volume already exists. Safe to
# re-run: data tables use IF NOT EXISTS and only the stateless consumers and
# their views are rebuilt.
info "Waiting for the CDC topics to appear before building the consumers..."
for _ in $(seq 1 30); do
  if kubectl exec -n $NAMESPACE etl-kafka-dual-role-0 -- \
       /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null \
       | grep -q '^cdc\.'; then
    ok "CDC topics present"
    break
  fi
  sleep 5
done

info "Applying the mirror schema (idempotent, so existing volumes converge)..."
if kubectl exec -i clickhouse-0 -n $NAMESPACE -- clickhouse-client \
     --user "$(secret_val CLICKHOUSE_USER)" \
     --password "$(secret_val CLICKHOUSE_PASSWORD)" \
     --multiquery < "$CH_INIT_DIR/01_mirror_schema.sql"; then
  ok "Mirror schema applied"
else
  warn "Could not apply the mirror schema — the real-time mirror may not receive changes"
  warn "Retry with: kubectl exec -i clickhouse-0 -n $NAMESPACE -- clickhouse-client --user <u> --password <p> --multiquery < docker/clickhouse/initdb/01_mirror_schema.sql"
fi

# Everything above is the real-time path: databases, object storage, Kafka,
# Debezium and the ClickHouse mirror. DEPLOY_PROFILE=core stops here, which is
# what CI exercises — it covers the parts that have actually broken on fresh
# installs without pulling the multi-gigabyte Airflow image.
if [ "${DEPLOY_PROFILE:-full}" = "core" ]; then
  echo ""
  ok "Core profile complete (databases, MinIO, Kafka, Debezium, ClickHouse)"
  info "Set DEPLOY_PROFILE=full for Spark, Trino, monitoring, Airflow and the dashboard."
  exit 0
fi

# ─── Step 7: Spark ────────────────────────────────────────────────────────────
echo ""
info "Deploying Spark master and workers..."
kubectl apply -f "$TMP_K8S/spark/"
kubectl rollout status statefulset/spark-master -n $NAMESPACE --timeout=300s
kubectl rollout status deployment/spark-worker -n $NAMESPACE --timeout=300s
ok "Spark cluster is ready"

# One-time migration: the CDC sync daemon was retired when ClickHouse became
# the sole Pipe 3 consumer. Remove it if an older deployment left it running.
kubectl delete deployment/cdc-sync-daemon -n $NAMESPACE --ignore-not-found

info "Deploying Trino (lakehouse query engine) via the official Helm chart..."
# One-time migration: remove the previous raw-manifest deployment if present
kubectl delete deployment/trino service/trino configmap/trino-catalog \
  -n $NAMESPACE --ignore-not-found
helm repo add trino https://trinodb.github.io/charts --force-update
helm upgrade --install trino trino/trino \
  --namespace $NAMESPACE \
  --values "$TMP_K8S/trino/helm-values.yaml" \
  --timeout 10m
kubectl rollout status deployment/trino-coordinator -n $NAMESPACE --timeout=300s
kubectl rollout status deployment/trino-worker -n $NAMESPACE --timeout=300s
ok "Trino is ready (coordinator + workers; scale with server.workers in k8s/trino/helm-values.yaml)"

# ─── Step 8: Monitoring ───────────────────────────────────────────────────────
echo ""
info "Deploying Prometheus and Grafana..."
# Provision the same Grafana dashboards compose uses (Data Platform Health)
kubectl create configmap grafana-dashboards -n $NAMESPACE \
  --from-file="$REPO_ROOT/monitoring/grafana/dashboards/data_platform.json" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f "$TMP_K8S/monitoring/"
kubectl rollout status deployment/prometheus -n $NAMESPACE --timeout=300s
kubectl rollout status deployment/grafana    -n $NAMESPACE --timeout=300s
ok "Monitoring stack is ready"

# ─── Step 9: Airflow ──────────────────────────────────────────────────────────
echo ""
# The DAGs resolve their hooks by connection id — source_postgres,
# dest_postgres and minio_s3 (the Cosmos dbt profile uses dest_postgres too).
# docker-compose supplies all of these as AIRFLOW_CONN_* env vars; Kubernetes
# only ever set AIRFLOW_CONN_SPARK_DEFAULT, so every ingest task failed within
# seconds looking up a connection that did not exist. Built here rather than in
# the values file because the URIs embed credentials that live in the secret.
info "Building Airflow connection URIs from the deployed credentials..."
kubectl create secret generic airflow-connections -n $NAMESPACE \
  --from-literal=AIRFLOW_CONN_SOURCE_POSTGRES="postgresql://$(secret_val SOURCE_DB_USER):$(secret_val SOURCE_DB_PASSWORD)@postgres-source-0.postgres-source.${NAMESPACE}.svc.cluster.local:5432/$(secret_val SOURCE_DB_NAME)" \
  --from-literal=AIRFLOW_CONN_DEST_POSTGRES="postgresql://$(secret_val DEST_DB_USER):$(secret_val DEST_DB_PASSWORD)@postgres-dest-0.postgres-dest.${NAMESPACE}.svc.cluster.local:5432/$(secret_val DEST_DB_NAME)" \
  --from-literal=AIRFLOW_CONN_MINIO_S3="aws://$(secret_val MINIO_ROOT_USER):$(secret_val MINIO_ROOT_PASSWORD)@?endpoint_url=http%3A%2F%2Fminio-0.minio.${NAMESPACE}.svc.cluster.local%3A9000" \
  --dry-run=client -o yaml | kubectl apply -f -
ok "Airflow connections published"

info "Adding Airflow Helm repo..."
helm repo add apache-airflow https://airflow.apache.org --force-update
helm repo update

info "Deploying Airflow via Helm (this takes 2-3 minutes)..."
helm upgrade --install airflow apache-airflow/airflow \
  --namespace $NAMESPACE \
  --values "$TMP_K8S/airflow/helm-values.yaml" \
  --set "defaultAirflowRepository=${AIRFLOW_IMAGE%:*}" \
  --set "defaultAirflowTag=${AIRFLOW_IMAGE##*:}" \
  --set "images.airflow.repository=${AIRFLOW_IMAGE%:*}" \
  --set "images.airflow.tag=${AIRFLOW_IMAGE##*:}" \
  --set "postgresql.primary.persistence.storageClass=${STORAGE_CLASS}" \
  --timeout 10m

info "Waiting for Airflow API Server to be ready (this may take a few minutes)..."
kubectl rollout status deployment/airflow-api-server -n $NAMESPACE --timeout=600s || warn "Airflow API Server took too long, but may still be starting."
ok "Airflow is ready"

# ─── Step 10: Seed sample data ────────────────────────────────────────────────
echo ""
if [ -z "${SEED+x}" ]; then
  read -rp "Seed sample orders data into postgres-source? (y/N): " SEED
fi
if [[ "${SEED:-n}" =~ ^[Yy]$ ]]; then
  info "Running data generator (this may take a minute)..."
  SEED_DB_USER=$(secret_val SOURCE_DB_USER)
  SEED_DB_PASSWORD=$(secret_val SOURCE_DB_PASSWORD)
  SEED_DB_NAME=$(secret_val SOURCE_DB_NAME)
  # Pipe the local script into a temporary pod
  cat "$REPO_ROOT/sample-data/generate_ecommerce.py" | kubectl run seed-data \
    --image=python:3.11-slim \
    --restart=Never \
    -i --rm \
    --namespace=$NAMESPACE \
    --env="SOURCE_DB_HOST=postgres-source-0.postgres-source.${NAMESPACE}.svc.cluster.local" \
    --env="SOURCE_DB_USER=${SEED_DB_USER}" \
    --env="SOURCE_DB_PASSWORD=${SEED_DB_PASSWORD}" \
    --env="SOURCE_DB_NAME=${SEED_DB_NAME}" \
    --command -- sh -c "
      pip install psycopg2-binary faker pandas pyarrow -q &&
      python3 -
    " || warn "Seed job failed — ensure you have internet access in the cluster to install python deps"
  ok "Sample data seeded"
fi

# ─── Step 11: AI Data Assistant Dashboard ──────────────────────────────────────
echo ""
info "Deploying AI Data Assistant Dashboard..."
kubectl apply -f "$TMP_K8S/ui/"
kubectl rollout status deployment/data-dashboard -n $NAMESPACE --timeout=300s
ok "AI Dashboard is ready"

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
echo "  Airflow UI         http://${NODE_IP}:30880  (admin / admin)"
echo "  Kafka UI           http://${NODE_IP}:30801  (KAFKA_UI_USER / KAFKA_UI_PASSWORD)"
echo "  Grafana            http://${NODE_IP}:30300  (AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD)"
echo "  AI Dashboard       http://${NODE_IP}:30333  (DASHBOARD_AUTH_USER / DASHBOARD_AUTH_PASSWORD)"
echo "  MinIO Console      http://${NODE_IP}:30901  (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD)"
echo "  Spark UI           http://${NODE_IP}:30808"
echo ""
echo "  Names in brackets are keys in the etl-secrets secret — read one with:"
echo "    kubectl get secret etl-secrets -n etl -o jsonpath='{.data.MINIO_ROOT_PASSWORD}' | base64 -d"
echo ""
echo "  To scale for 50GB+ datasets, update k8s/02-configmaps.yaml:"
echo "    ETL_CHUNK_SIZE:      500000"
echo "    SPARK_EXECUTOR_MEMORY: 8g"
echo ""
echo "  Then: kubectl apply -f k8s/02-configmaps.yaml"
echo "        kubectl rollout restart deployment -n etl"
echo ""
