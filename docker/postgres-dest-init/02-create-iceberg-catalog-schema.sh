#!/bin/bash
# Creates the schema that holds Iceberg JDBC catalog metadata
# (iceberg_tables / iceberg_namespace_properties), shared by Spark and Trino.
#
# Runs automatically on a fresh postgres-dest volume. For an EXISTING volume:
#   docker exec -it postgres-dest bash /docker-entrypoint-initdb.d/02-create-iceberg-catalog-schema.sh
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS iceberg_catalog AUTHORIZATION ${POSTGRES_USER};
EOSQL

echo "Schema iceberg_catalog is ready."
