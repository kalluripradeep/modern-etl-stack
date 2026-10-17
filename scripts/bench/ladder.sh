#!/usr/bin/env bash
# Throughput and freshness ladder for the CDC pipeline.
#
# Runs scripts/bench/orders.sql at increasing client counts and records, for
# each arm, what the source sustained (pgbench TPS and latency) alongside what
# the pipeline did about it (end-to-end freshness, sampled while the load is
# running rather than after it stops).
#
# Those are two different ceilings and they are not the same number. Postgres
# saturating is visible in the TPS column flattening; the pipeline falling
# behind is visible in freshness growing instead of holding flat. Measuring
# only the first is how a pipeline gets called "fast" while its mirror drifts.
#
#   bash scripts/bench/ladder.sh
#   DURATION=120 CLIENTS="8 16 32" bash scripts/bench/ladder.sh
#   RATE=500 CLIENTS=16 bash scripts/bench/ladder.sh   # fixed rate, not saturation
#
# Targets Compose if postgres-source is running there, otherwise the etl
# namespace. Reads nothing and writes only orders -- but it writes a lot of
# them, so point it at a sandbox.
set -uo pipefail

DURATION=${DURATION:-60}
CLIENTS=${CLIENTS:-"1 4 8 16 32"}
RATE=${RATES:-${RATE:-}}
SQL="$(cd "$(dirname "$0")" && pwd)/orders.sql"
OUT=${OUT:-bench_ladder.tsv}

# Both stacks are often up at once on a dev machine, so the guess is only a
# default -- TARGET=k8s or TARGET=compose settles it.
if [ "${TARGET:-}" = "compose" ] || { [ -z "${TARGET:-}" ] && docker ps --format '{{.Names}}' 2>/dev/null | grep -qx postgres-source; }; then
  TARGET="compose"
  PGPW=${SOURCE_DB_PASSWORD:-sourcepass}
  docker cp "$SQL" postgres-source:/tmp/ladder.sql >/dev/null
  pg() { MSYS_NO_PATHCONV=1 docker exec -e PGPASSWORD="$PGPW" postgres-source pgbench "$@"; }
  ch() { docker exec clickhouse clickhouse-client --user "${CLICKHOUSE_USER:-chuser}" --password "${CLICKHOUSE_PASSWORD:-chpass}" -q "$1"; }
else
  TARGET="k8s (namespace ${NS:=etl})"
  PGPW=$(kubectl get secret etl-secrets -n "$NS" -o jsonpath='{.data.SOURCE_DB_PASSWORD}' | base64 -d)
  CHPW=$(kubectl get secret etl-secrets -n "$NS" -o jsonpath='{.data.CLICKHOUSE_PASSWORD}' | base64 -d)
  # MSYS_NO_PATHCONV stops Git Bash on Windows rewriting the in-container
  # /tmp path into a Windows one before kubectl ever sees it. It is an
  # ordinary env var, so it is simply ignored everywhere else.
  # Copied from the script's own directory by relative name: MSYS_NO_PATHCONV
  # suppresses path conversion for every argument, so an absolute local source
  # would stay POSIX and kubectl would look for a Windows path that is not
  # there. A relative source is never converted in the first place.
  ( cd "$(dirname "$0")" && MSYS_NO_PATHCONV=1 kubectl cp orders.sql "$NS/postgres-source-0:/tmp/ladder.sql" ) >/dev/null
  pg() { MSYS_NO_PATHCONV=1 kubectl exec -n "$NS" postgres-source-0 -- env PGPASSWORD="$PGPW" pgbench "$@"; }
  ch() { MSYS_NO_PATHCONV=1 kubectl exec -i -n "$NS" clickhouse-0 -- clickhouse-client --user chuser --password "$CHPW" -q "$1"; }
fi

FRESH_SQL="SELECT dateDiff('second', max(updated_at), now()) FROM mirror.orders"
ch "SELECT 1" >/dev/null 2>&1 || { echo "ClickHouse unreachable on $TARGET — freshness cannot be measured"; exit 1; }

echo "target: $TARGET   duration: ${DURATION}s per arm   rate: ${RATE:-saturation}"
printf 'clients\ttps\tlat_ms\tfresh_med_s\tfresh_max_s\tfailed\n' | tee "$OUT"

for C in $CLIENTS; do
  J=$(( C < 4 ? 1 : 4 ))
  SAMPLES=$(mktemp)
  ( while :; do ch "$FRESH_SQL" 2>/dev/null | tr -d '\r' >> "$SAMPLES"; sleep 5; done ) &
  SAMPLER=$!

  ERRS=$(mktemp)
  RES=$(pg -h 127.0.0.1 -U "${SOURCE_DB_USER:-sourceuser}" -d "${SOURCE_DB_NAME:-sourcedb}" \
          -f /tmp/ladder.sql -c "$C" -j "$J" -T "$DURATION" --no-vacuum \
          ${RATE:+-R "$RATE"} 2>"$ERRS")

  kill "$SAMPLER" 2>/dev/null; wait "$SAMPLER" 2>/dev/null

  TPS=$(printf '%s' "$RES" | sed -n 's/^tps = \([0-9.]*\).*/\1/p')

  # An arm that could not run at all must say so. pgbench exits non-zero and
  # prints nothing parseable when, for instance, the client count exceeds
  # max_connections -- reporting that as 0 tps reads like a measured collapse
  # rather than a configuration limit that was never exercised.
  if [ -z "$TPS" ]; then
    printf '%s\tFAILED\t-\t-\t-\t-\n' "$C" | tee -a "$OUT"
    sed -n 's/^pgbench: error: //p' "$ERRS" | head -1 | sed 's/^/  /'
    rm -f "$ERRS" "$SAMPLES"
    continue
  fi
  rm -f "$ERRS"
  LAT=$(printf '%s' "$RES" | sed -n 's/^latency average = \([0-9.]*\).*/\1/p')
  FAIL=$(printf '%s' "$RES" | sed -n 's/^number of failed transactions: \([0-9]*\).*/\1/p')

  # Median and max over the second half of the samples: the first half still
  # includes the pipeline absorbing the step up in load, which is a transient,
  # not the steady state the arm is meant to characterise.
  # sort(1) rather than awk's asort, which is a gawk extension -- Debian ships
  # mawk by default and this would quietly report nothing there.
  HALF=$(awk 'NF{n++} END{print int(n/2)}' "$SAMPLES")
  STEADY=$(awk -v h="$HALF" 'NF && NR>h' "$SAMPLES" | sort -n)
  MED=$(printf '%s
' "$STEADY" | awk 'NF{a[n++]=$1} END{if(!n){print "-";exit} print (n%2 ? a[int(n/2)] : (a[n/2-1]+a[n/2])/2)}')
  MAX=$(printf '%s
' "$STEADY" | awk 'NF{m=$1} END{print (NR?m:"-")}')
  rm -f "$SAMPLES"

  printf '%s\t%.0f\t%.1f\t%s\t%s\t%s\n' "$C" "${TPS:-0}" "${LAT:-0}" "${MED:--}" "${MAX:--}" "${FAIL:--}" | tee -a "$OUT"
done

echo ""
echo "Wrote $OUT"
echo "Read it as two ceilings: tps flattening is Postgres saturating; fresh_med_s"
echo "growing arm over arm is the pipeline falling behind. They rarely coincide."
