#!/usr/bin/env bash
# Register the Debezium PostgreSQL connector with Kafka Connect.
# Run this once after `make up` and the stack is healthy.
# Usage: bash scripts/register_debezium_connector.sh

set -euo pipefail

CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"

SOURCE_DB_HOST="${SOURCE_DB_HOST:-postgres-source}"
SOURCE_DB_PORT="${SOURCE_DB_PORT:-5432}"
SOURCE_DB_USER="${SOURCE_DB_USER:-sourceuser}"
SOURCE_DB_PASSWORD="${SOURCE_DB_PASSWORD:-sourcepass}"
SOURCE_DB_NAME="${SOURCE_DB_NAME:-sourcedb}"

echo "Waiting for Kafka Connect to be ready..."
until curl -sf "${CONNECT_URL}/connectors" > /dev/null; do
  echo "  ... not ready yet, retrying in 5s"
  sleep 5
done
echo "Kafka Connect is ready."

# Check if connector already exists
if curl -sf "${CONNECT_URL}/connectors/orders-cdc-connector" > /dev/null 2>&1; then
  echo "Connector 'orders-cdc-connector' already exists. Delete it first to re-register:"
  echo "  curl -X DELETE ${CONNECT_URL}/connectors/orders-cdc-connector"
  exit 0
fi

echo "Registering Debezium PostgreSQL connector..."

curl -X POST "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"orders-cdc-connector\",
    \"config\": {
      \"connector.class\": \"io.debezium.connector.postgresql.PostgresConnector\",
      \"database.hostname\": \"${SOURCE_DB_HOST}\",
      \"database.port\": \"${SOURCE_DB_PORT}\",
      \"database.user\": \"${SOURCE_DB_USER}\",
      \"database.password\": \"${SOURCE_DB_PASSWORD}\",
      \"database.dbname\": \"${SOURCE_DB_NAME}\",
      \"topic.prefix\": \"cdc\",
      \"table.include.list\": \"public.orders\",
      \"plugin.name\": \"pgoutput\",
      \"slot.name\": \"debezium_orders_slot\",
      \"publication.name\": \"debezium_orders_pub\",
      \"snapshot.mode\": \"initial\"
    }
  }"

echo ""
echo "Connector registered. Check status with:"
echo "  curl ${CONNECT_URL}/connectors/orders-cdc-connector/status | jq ."
