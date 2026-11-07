#!/usr/bin/env bash
# Pipe 3 drain-rate benchmark: how fast can ClickHouse consume, on its own?
#
# scripts/bench/ladder.sh measures the source and the pipeline together, so a
# slow consumer and a slow producer look alike -- freshness grows either way.
# This isolates the consumer. It stops ClickHouse reading, builds a known
# backlog in Kafka, then starts it again and times how long the backlog takes
# to clear. Nothing is produced during the timed part, so the number is the
# consumer's ceiling and nothing else.
#
#   bash scripts/bench/drain.sh
#   TARGET=k8s CLIENTS=64 BUILD=90 bash scripts/bench/drain.sh
#
# Measured with this on identical configuration -- clickhouse-server:25.3,
# one partition, kafka_num_consumers unset, 2 CPU and 4Gi limits:
#
#     docker compose on a laptop    32,312 msg/s
#     k8s on kangra (#188)           ~1,750 msg/s
#
# An 18x gap on the same settings is why the partition and consumer count are
# not the thing to change. See docs/BENCHMARKING.md.
#
# Writes orders, drops and recreates mirror.orders_mv. Point it at a sandbox.
set -uo pipefail

CLIENTS=${CLIENTS:-64}
BUILD=${BUILD:-90}
GROUP=${GROUP:-clickhouse-mirror-orders}
TOPIC=${TOPIC:-cdc.public.orders}
SQL="$(cd "$(dirname "$0")" && pwd)/orders.sql"
SCHEMA="$(cd "$(dirname "$0")/../.." && pwd)/docker/clickhouse/initdb/01_mirror_schema.sql"

if [ "${TARGET:-}" = "compose" ] || { [ -z "${TARGET:-}" ] && docker ps --format '{{.Names}}' 2>/dev/null | grep -qx postgres-source; }; then
  TARGET="compose"
  # No MSYS_NO_PATHCONV here, unlike the kubectl cp below. docker cp on
  # Windows wants a Windows path, so Git Bash converting this absolute POSIX
  # source is what makes it work; suppressing the conversion fails with
  # "GetFileAttributesEx C:\c:".
  docker cp "$SQL" postgres-source:/tmp/drain.sql >/dev/null
  pg() { MSYS_NO_PATHCONV=1 docker exec -e PGPASSWORD="${SOURCE_DB_PASSWORD:-sourcepass}" postgres-source pgbench "$@"; }
  ch() { docker exec -i clickhouse clickhouse-client --user "${CLICKHOUSE_USER:-chuser}" --password "${CLICKHOUSE_PASSWORD:-chpass}" "$@"; }
  kafka_groups() { docker exec kafka kafka-consumer-groups "$@"; }
else
  TARGET="k8s (namespace ${NS:=etl})"
  PGPW=$(kubectl get secret etl-secrets -n "$NS" -o jsonpath='{.data.SOURCE_DB_PASSWORD}' | base64 -d)
  CHPW=$(kubectl get secret etl-secrets -n "$NS" -o jsonpath='{.data.CLICKHOUSE_PASSWORD}' | base64 -d)
  # Relative source, so Git Bash leaves it alone and kubectl sees a POSIX
  # path -- the opposite of the docker cp above, for the opposite reason.
  ( cd "$(dirname "$0")" && MSYS_NO_PATHCONV=1 kubectl cp orders.sql "$NS/postgres-source-0:/tmp/drain.sql" ) >/dev/null
  pg() { MSYS_NO_PATHCONV=1 kubectl exec -n "$NS" postgres-source-0 -- env PGPASSWORD="$PGPW" pgbench "$@"; }
  ch() { MSYS_NO_PATHCONV=1 kubectl exec -i -n "$NS" clickhouse-0 -- clickhouse-client --user chuser --password "$CHPW" "$@"; }
  # Strimzi names the broker pod after the cluster and its node pool.
  kafka_groups() { MSYS_NO_PATHCONV=1 kubectl exec -n "$NS" "${KAFKA_POD:-etl-kafka-dual-role-0}" -- bin/kafka-consumer-groups.sh "$@"; }
fi

# Summed over partitions, so this keeps working if the topic is ever split.
lag() { kafka_groups --bootstrap-server localhost:9092 --describe --group "$GROUP" 2>/dev/null \
        | awk -v t="$TOPIC" '$2==t{s+=$6} END{print s+0}'; }

ch -q "SELECT 1" >/dev/null 2>&1 || { echo "ClickHouse unreachable on $TARGET"; exit 1; }

echo "target: $TARGET   build: ${BUILD}s at ${CLIENTS} clients"

# The Kafka engine only consumes while something reads from it, so dropping
# the view is enough to stop it. Offsets stay committed in Kafka under the
# same group name, so recreating the view resumes rather than replaying.
echo "stopping the consumer..."
ch -q "DROP VIEW IF EXISTS mirror.orders_mv"

echo "building a backlog..."
# A pgbench that cannot start leaves no backlog, which further down is
# indistinguishable from a dead connector. Say which one it was.
BUILD_OUT=$(pg -h 127.0.0.1 -U "${SOURCE_DB_USER:-sourceuser}" -d "${SOURCE_DB_NAME:-sourcedb}" \
   -f /tmp/drain.sql -c "$CLIENTS" -j 4 -T "$BUILD" --no-vacuum 2>&1)
TPS=$(printf '%s' "$BUILD_OUT" | sed -n 's/^tps = \([0-9.]*\).*/\1/p')
if [ -z "$TPS" ]; then
  echo "ABORTING: pgbench did not run, so there is no backlog to time."
  printf '%s\n' "$BUILD_OUT" | sed -n 's/^pgbench: error: /  /p' | head -3
  exit 1
fi
echo "  source produced $TPS tps"

# Debezium is still reading the WAL when pgbench stops, so the backlog keeps
# growing for a few seconds. Wait for it to stop moving rather than guessing.
PREV=-1
for _ in $(seq 1 30); do
  BACKLOG=$(lag)
  [ "$BACKLOG" = "$PREV" ] && break
  PREV=$BACKLOG
  sleep 3
done
echo "  backlog settled at $BACKLOG messages"

if [ "${BACKLOG:-0}" -lt 1000 ]; then
  echo ""
  echo "ABORTING: the source wrote $TPS tps but only $BACKLOG messages reached"
  echo "  Kafka, so there is no backlog to time. A stopped connector task is"
  echo "  the usual reason. Read the SECOND state in its status:"
  echo ""
  echo "    kubectl exec -n ${NS:-etl} kafka-connect-0 -- curl -s localhost:8083/connectors/orders-cdc-connector/status"
  exit 1
fi

echo "starting the consumer..."
START=$(date +%s)
ELAPSED=0
sed -n '/CREATE MATERIALIZED VIEW mirror.orders_mv/,/;$/p' "$SCHEMA" | ch --multiquery

while :; do
  L=$(lag)
  ELAPSED=$(( $(date +%s) - START ))
  printf '  t=%ss  lag=%s\n' "$ELAPSED" "$L"
  [ "$L" -lt 2000 ] && break
  [ "$ELAPSED" -gt "${TIMEOUT:-600}" ] && echo "  giving up after ${ELAPSED}s" && break
  sleep 5
done

[ "$ELAPSED" -lt 1 ] && ELAPSED=1
echo ""
echo "drained $BACKLOG messages in ${ELAPSED}s = $((BACKLOG / ELAPSED)) msg/s"
echo ""
echo "orders.sql writes two messages per transaction, so a ladder arm at N tps"
echo "offers 2N msg/s to this consumer. Compare the two before changing"
echo "partitions or kafka_num_consumers: neither helps a consumer that is"
echo "already draining faster than the source can produce."
