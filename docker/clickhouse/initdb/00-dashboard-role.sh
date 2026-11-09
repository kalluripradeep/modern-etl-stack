#!/bin/bash
# Creates the read-only ClickHouse role the AI dashboard connects as (#231
# phase 4). The grants it gets are generated into 01_mirror_schema.sql from
# the data contract; this only creates the login, because a password does not
# belong in a generated file that is committed.
#
# Runs automatically on first boot of a fresh ClickHouse volume. For an
# existing volume, apply it by hand:
#   docker exec -e DASHBOARD_CH_PASSWORD=... clickhouse bash \
#     /docker-entrypoint-initdb.d/00-dashboard-role.sh
#
# The 00 prefix matters: the grants in 01_mirror_schema.sql name this role,
# and ClickHouse runs initdb files in order.
set -euo pipefail

DASHBOARD_CH_USER="${DASHBOARD_CH_USER:-dashboard_ro}"
DASHBOARD_CH_PASSWORD="${DASHBOARD_CH_PASSWORD:-dashboard_ro}"

clickhouse-client --user "${CLICKHOUSE_USER:-chuser}" --password "${CLICKHOUSE_PASSWORD:-chpass}" \
  -q "CREATE USER IF NOT EXISTS ${DASHBOARD_CH_USER} IDENTIFIED WITH plaintext_password BY '${DASHBOARD_CH_PASSWORD}'"

echo "Read-only ClickHouse role '${DASHBOARD_CH_USER}' is ready."
