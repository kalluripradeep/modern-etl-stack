#!/bin/bash
# Creates a read-only role for the AI dashboard on the destination warehouse.
#
# Runs automatically on first boot of a fresh postgres-dest volume
# (mounted into /docker-entrypoint-initdb.d/). For an EXISTING volume,
# apply it manually instead:
#   docker exec -it postgres-dest bash /docker-entrypoint-initdb.d/01-create-dashboard-role.sh
set -euo pipefail

DASHBOARD_DB_USER="${DASHBOARD_DB_USER:-dashboard_ro}"
DASHBOARD_DB_PASSWORD="${DASHBOARD_DB_PASSWORD:-dashboard_ro}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${DASHBOARD_DB_USER}') THEN
            CREATE ROLE ${DASHBOARD_DB_USER} WITH LOGIN PASSWORD '${DASHBOARD_DB_PASSWORD}';
        END IF;
    END
    \$\$;

    GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO ${DASHBOARD_DB_USER};

    -- The dashboard queries the dbt layers (raw/int/prs), not just public.
    -- Create the schemas up front so USAGE can be granted on a fresh volume;
    -- dbt reuses them.
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS int;
    CREATE SCHEMA IF NOT EXISTS prs;
    CREATE SCHEMA IF NOT EXISTS gold;
    GRANT USAGE ON SCHEMA public, raw, int, prs, gold TO ${DASHBOARD_DB_USER};

    -- No table grants here, and deliberately no ALTER DEFAULT PRIVILEGES.
    --
    -- This used to GRANT SELECT ON ALL TABLES and set default privileges so
    -- that everything dbt created later was readable too. That is what made
    -- the data contract decorative: `email` was as readable as `order_id`,
    -- and narrowing the grants would have been undone by the next dbt run.
    --
    -- Which columns this role may read is decided by the classifications in
    -- airflow/dags/config/pipelines.yml and applied by the
    -- apply_access_policy task at the end of every ingest run (#231 phase 4).
    -- Until that has run once the role can read nothing, which is the right
    -- way for this to fail and is also before there is anything to read.
EOSQL

echo "Read-only dashboard role '${DASHBOARD_DB_USER}' is ready."
