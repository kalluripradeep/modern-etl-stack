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
    -- Create the schemas up front so the grants apply on a fresh volume;
    -- dbt reuses them. Default privileges cover tables created later.
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS int;
    CREATE SCHEMA IF NOT EXISTS prs;
    GRANT USAGE ON SCHEMA public, raw, int, prs TO ${DASHBOARD_DB_USER};
    GRANT SELECT ON ALL TABLES IN SCHEMA public, raw, int, prs TO ${DASHBOARD_DB_USER};
    ALTER DEFAULT PRIVILEGES FOR ROLE ${POSTGRES_USER} IN SCHEMA public
        GRANT SELECT ON TABLES TO ${DASHBOARD_DB_USER};
    ALTER DEFAULT PRIVILEGES FOR ROLE ${POSTGRES_USER} IN SCHEMA raw
        GRANT SELECT ON TABLES TO ${DASHBOARD_DB_USER};
    ALTER DEFAULT PRIVILEGES FOR ROLE ${POSTGRES_USER} IN SCHEMA int
        GRANT SELECT ON TABLES TO ${DASHBOARD_DB_USER};
    ALTER DEFAULT PRIVILEGES FOR ROLE ${POSTGRES_USER} IN SCHEMA prs
        GRANT SELECT ON TABLES TO ${DASHBOARD_DB_USER};
EOSQL

echo "Read-only dashboard role '${DASHBOARD_DB_USER}' is ready."
