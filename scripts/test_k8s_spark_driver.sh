#!/usr/bin/env bash
# test_k8s_spark_driver.sh — prove the spark.driver.host fix on real Kubernetes
#
# WHY THIS EXISTS
#   Every Spark fix so far was verified under docker-compose, where the driver's
#   hostname is the container name and Docker's DNS resolves it. That hides the
#   bug this tests for. Under KubernetesExecutor the driver runs in a bare task
#   pod, which has no Service, so nothing in the cluster can resolve its name --
#   executors cannot call back, and they die on loop.
#
# WHAT IT DOES
#   Runs the same job twice from a bare pod (the shape of a KubernetesExecutor
#   task pod), differing only in whether spark.driver.host is set:
#
#     NEGATIVE  no spark.driver.host  -> driver advertises its pod hostname,
#                                        executors cannot reach it, no result
#     POSITIVE  spark.driver.host=IP  -> driver advertises its pod IP,
#                                        executors connect, job completes
#
#   The negative case is the point. A harness that cannot fail proves nothing,
#   so if NEGATIVE unexpectedly passes, this script says so loudly and treats
#   the whole run as inconclusive.
#
# WHY SparkPi AND NOT THE REAL JOB
#   SparkPi ships inside the Spark image and needs no --packages, so Ivy never
#   runs. The real transform jobs pull Iceberg and hadoop-aws at submit time,
#   and in a bare pod Ivy fails for reasons that have nothing to do with this
#   bug: HOME is unset, and the image's non-root UID has no /etc/passwd entry
#   so the JVM cannot resolve user.home. Those are harness artifacts. SparkPi
#   exercises the one thing under test -- executors calling back to the driver.
#
# USAGE
#   bash scripts/test_k8s_spark_driver.sh
#
# REQUIREMENTS
#   kubectl pointed at a cluster that can pull bitnamilegacy/spark:3.5.0

set -uo pipefail

NAMESPACE="${NAMESPACE:-etl-sparktest}"
SPARK_IMAGE="bitnamilegacy/spark:3.5.0"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# The negative case hangs rather than erroring -- the driver sits logging
# "Initial job has not accepted any resources" while executors die and respawn.
# Bound it so the script terminates.
NEG_TIMEOUT="${NEG_TIMEOUT:-180}"
POS_TIMEOUT="${POS_TIMEOUT:-300}"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; BLUE='\033[0;34m'; NC='\033[0m'
info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $*"; }
die()   { fail "$*"; exit 1; }

command -v kubectl >/dev/null || die "kubectl not found"
kubectl cluster-info >/dev/null 2>&1 || die "no reachable cluster (check kubectl config current-context)"
ok "cluster reachable: $(kubectl config current-context)"

# ── 1. Namespace and the env the worker manifest expects ────────────────────
# worker-deployment.yaml has envFrom referencing etl-env and etl-secrets. They
# only carry MinIO/S3A settings, which SparkPi never touches, but a missing
# reference leaves the pod in CreateContainerConfigError -- so stub them.
info "Creating namespace $NAMESPACE and stub env"
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NAMESPACE" create configmap etl-env --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NAMESPACE" create secret generic etl-secrets --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# ── 2. Deploy Spark from the real manifests ─────────────────────────────────
# Namespace is rewritten so this never touches a real etl deployment.
info "Deploying Spark from k8s/spark/*.yaml (namespace rewritten to $NAMESPACE)"
for f in master-deployment.yaml worker-deployment.yaml service.yaml; do
  sed "s/namespace: etl$/namespace: $NAMESPACE/" "$REPO_ROOT/k8s/spark/$f" \
    | kubectl apply -n "$NAMESPACE" -f - >/dev/null || die "failed to apply $f"
done

info "Waiting for spark-master (up to 5m -- first run pulls the image)"
kubectl -n "$NAMESPACE" rollout status statefulset/spark-master --timeout=300s \
  || die "spark-master did not become ready"
info "Waiting for spark-worker"
kubectl -n "$NAMESPACE" rollout status deployment/spark-worker --timeout=300s \
  || die "spark-worker did not become ready"

DEPLOYED_IMAGE=$(kubectl -n "$NAMESPACE" get pods -l app=spark-worker \
  -o jsonpath='{.items[0].spec.containers[0].image}')
ok "workers running $DEPLOYED_IMAGE"
[ "$DEPLOYED_IMAGE" = "$SPARK_IMAGE" ] \
  || warn "expected $SPARK_IMAGE -- version parity (#130) may not be in this tree"

# ── 3. The test itself ──────────────────────────────────────────────────────
# Bare pod, no Service, restartPolicy=Never: the shape of a KubernetesExecutor
# task pod. That absence of a Service is precisely what makes driver.host matter.
run_case() {
  local name="$1" driver_conf="$2" timeout_s="$3" podname="sparktest-$1-$RANDOM"

  info "── $name case: submitting SparkPi from a bare pod (no Service)"
  timeout "$timeout_s" kubectl -n "$NAMESPACE" run "$podname" \
    --image="$SPARK_IMAGE" --restart=Never --attach --rm -q \
    --command -- bash -c "
      JAR=\$(ls /opt/bitnami/spark/examples/jars/spark-examples_*.jar 2>/dev/null | head -1)
      echo \"POD_HOSTNAME=\$(hostname)\"
      echo \"POD_IP=\$(hostname -i | cut -d' ' -f1)\"
      /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        $driver_conf \
        --conf spark.driver.bindAddress=0.0.0.0 \
        --conf spark.executor.memory=1g \
        --conf spark.cores.max=2 \
        --class org.apache.spark.examples.SparkPi \
        \$JAR 10 2>&1
    " 2>&1
  return $?
}

echo
echo "═══════════════════════════════════════════════════════════════════"
echo " NEGATIVE — driver advertises its pod hostname (the bug)"
echo "═══════════════════════════════════════════════════════════════════"
NEG_OUT=$(run_case "negative" "" "$NEG_TIMEOUT"); NEG_RC=$?
echo "$NEG_OUT" | tail -25
kubectl -n "$NAMESPACE" delete pod -l run --ignore-not-found >/dev/null 2>&1

echo
echo "═══════════════════════════════════════════════════════════════════"
echo " POSITIVE — driver advertises its pod IP (the fix, as shipped in #129)"
echo "═══════════════════════════════════════════════════════════════════"
POS_OUT=$(run_case "positive" \
  '--conf spark.driver.host=$(hostname -i | cut -d" " -f1)' "$POS_TIMEOUT"); POS_RC=$?
echo "$POS_OUT" | tail -25

# ── 4. Verdict ──────────────────────────────────────────────────────────────
echo
echo "═══════════════════════════════════════════════════════════════════"
echo " VERDICT"
echo "═══════════════════════════════════════════════════════════════════"
neg_got_pi=0; pos_got_pi=0
grep -q "Pi is roughly" <<<"$NEG_OUT" && neg_got_pi=1
grep -q "Pi is roughly" <<<"$POS_OUT" && pos_got_pi=1

echo "  negative: rc=$NEG_RC  computed Pi: $([ $neg_got_pi -eq 1 ] && echo YES || echo no)"
echo "  positive: rc=$POS_RC  computed Pi: $([ $pos_got_pi -eq 1 ] && echo YES || echo no)"
echo

if [ $neg_got_pi -eq 1 ]; then
  warn "INCONCLUSIVE — the negative case succeeded, so this harness does not"
  warn "reproduce the bug and proves nothing about the fix. Most likely the"
  warn "cluster resolves bare pod hostnames (some CNI/DNS setups do), which"
  warn "means it is not reproducing Raghu's environment."
  exit 2
elif [ $pos_got_pi -eq 1 ]; then
  ok "CONFIRMED — negative fails, positive succeeds."
  ok "spark.driver.host=\$(hostname -i) is what makes executors reach the driver."
  exit 0
else
  fail "BOTH cases failed. The fix is not the differentiator here -- something"
  fail "else is broken (workers unregistered, image pull, resources). Check:"
  fail "  kubectl -n $NAMESPACE get pods"
  fail "  kubectl -n $NAMESPACE logs -l app=spark-worker --tail=50"
  exit 1
fi
