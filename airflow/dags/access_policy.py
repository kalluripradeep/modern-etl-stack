"""Turn the data contract into grants the database enforces.

Phase 4 of #231. Phase 0 recorded which columns identify a person. Nothing
acted on it: `dashboard_ro` was created with GRANT SELECT ON ALL TABLES, so
the assistant could read `email` as readily as `order_id`.

Enforcement is in the database rather than in the API, and that is the whole
point. The assistant writes its own SQL, so anything checked in the
application has to be checked against generated SQL -- and a column alias
(`SELECT email AS e`), an expression (`substr(email, 1, 3)`) or a view all
defeat matching on names. Phase 3's exposure matcher carries a comment saying
it must not be used for access decisions for exactly this reason. Postgres
does not have that problem: a column grant holds however the query is
written.

Two deliberate choices, both in the direction of failing closed:

Denial is by column NAME across every schema the dashboard can reach, not by
table. The contract classifies the four source tables; dbt then copies those
columns into int, prs and gold under the same names, and classifying every
derived model by hand is a list that would silently fall behind. A name
collision -- some unrelated `state` column -- is over-denial, which is
visible and gets fixed. Under-denial is invisible, which is the failure that
matters.

Views are all-or-nothing. A Postgres view runs with its owner's privileges
unless it is declared security_invoker, so column grants underneath do not
protect it. Rather than depend on how each view was declared, any view whose
definition reads a denied column is not granted at all.
"""


SCHEMAS = ('public', 'raw', 'int', 'prs', 'gold', 'meta')

SENSITIVE = ('pii', 'phi')


def denied_columns(manifest):
    """Column names the dashboard role must not be able to read."""
    denied = set()
    for table in manifest['tables'].values():
        for classification, columns in (table.get('classification') or {}).items():
            if classification in SENSITIVE:
                denied.update(columns)
    return denied


def grants_for_tables(table_columns, denied, role):
    """One GRANT per table, naming only the columns that are allowed.

    A table whose every column is denied gets no grant at all rather than an
    empty column list, which Postgres rejects.
    """
    statements = []
    for (schema, table), columns in sorted(table_columns.items()):
        allowed = [c for c in columns if c not in denied]
        if not allowed:
            continue
        cols = ', '.join(f'"{c}"' for c in allowed)
        statements.append(f'GRANT SELECT ({cols}) ON "{schema}"."{table}" TO {role};')
    return statements


def grants_for_views(view_sources, denied, role):
    """All-or-nothing per view: a view reading a denied column is not granted.

    Column grants on the underlying table do not protect a view, because a
    view executes with its owner's privileges unless it was created
    security_invoker. Refusing the whole view does not depend on that.
    """
    statements = []
    for (schema, view), sources in sorted(view_sources.items()):
        if sources & denied:
            continue
        statements.append(f'GRANT SELECT ON "{schema}"."{view}" TO {role};')
    return statements


def schema_usage(role, schemas):
    """USAGE on every schema the policy covers.

    Without it a column grant is unreachable: the role is allowed to read the
    table and not allowed to enter the schema holding it. `meta` is the case
    that bites, because phase 1 creates it long after the role script granted
    USAGE on the five schemas it knew about -- so the assistant was granted
    SELECT on meta.dbt_quality and still could not read it, which made phase
    1's whole output invisible to the thing it was built for.

    Granting USAGE here rather than in the role script keeps it tied to
    whatever the policy actually covers, so the next schema does not repeat
    this.
    """
    return [
        f'GRANT USAGE ON SCHEMA "{schema}" TO {role};'
        for schema in sorted(schemas)
    ]


def revocations(role, schemas):
    """Start from nothing, every time.

    Only schemas that exist: `meta` is created by phase 1's capture task and
    is absent until that has run once, and REVOKE on a missing schema is an
    error rather than a no-op.

    The default privileges matter as much as the revokes. The role was set up
    with ALTER DEFAULT PRIVILEGES ... GRANT SELECT ON TABLES, so every table
    dbt created afterwards arrived fully readable -- narrowing the grants
    without removing that would be undone by the next dbt run.
    """
    statements = []
    for schema in sorted(schemas):
        statements.append(f'REVOKE ALL ON ALL TABLES IN SCHEMA "{schema}" FROM {role};')
    return statements


def plan(table_columns, view_sources, denied, role, schemas=None):
    """The whole policy as an ordered list of statements."""
    if schemas is None:
        schemas = {s for (s, _) in table_columns} | {s for (s, _) in view_sources}
    return (
        revocations(role, schemas)
        + schema_usage(role, schemas)
        + grants_for_tables(table_columns, denied, role)
        + grants_for_views(view_sources, denied, role)
    )


# ── Introspection ────────────────────────────────────────────────────────────
#
# What the dashboard can reach is read from the database rather than from the
# manifest. dbt creates int, prs and gold, and a model added next week has to
# be covered without anyone remembering to list it here.

_BASE_TABLES = """
SELECT c.table_schema, c.table_name, c.column_name
FROM information_schema.columns c
JOIN information_schema.tables t
  ON t.table_schema = c.table_schema AND t.table_name = c.table_name
WHERE c.table_schema = ANY(%s) AND t.table_type = 'BASE TABLE'
"""

_VIEW_SOURCES = """
SELECT v.table_schema, v.table_name, u.column_name
FROM information_schema.views v
LEFT JOIN information_schema.view_column_usage u
  ON u.view_schema = v.table_schema AND u.view_name = v.table_name
WHERE v.table_schema = ANY(%s)
"""


def existing_schemas(conn, schemas=SCHEMAS):
    with conn.cursor() as cur:
        cur.execute(
            'SELECT schema_name FROM information_schema.schemata '
            'WHERE schema_name = ANY(%s)', (list(schemas),)
        )
        return {row[0] for row in cur.fetchall()}


def read_catalog(conn, schemas=SCHEMAS):
    """Returns (table_columns, view_sources) for the reachable schemas."""
    table_columns = {}
    view_sources = {}
    with conn.cursor() as cur:
        cur.execute(_BASE_TABLES, (list(schemas),))
        for schema, table, column in cur.fetchall():
            table_columns.setdefault((schema, table), []).append(column)

        cur.execute(_VIEW_SOURCES, (list(schemas),))
        for schema, view, column in cur.fetchall():
            entry = view_sources.setdefault((schema, view), set())
            if column is not None:
                entry.add(column)
    return table_columns, view_sources


def apply(conn, manifest, role):
    """Re-derive and apply the policy. Returns what it did."""
    denied = denied_columns(manifest)
    present = existing_schemas(conn)
    table_columns, view_sources = read_catalog(conn, present)
    statements = plan(table_columns, view_sources, denied, role, present)

    try:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    withheld = sorted(
        f'{s}.{v}' for (s, v), src in view_sources.items() if src & denied
    )
    return {
        'denied_columns': sorted(denied),
        'tables': len(table_columns),
        'views': len(view_sources),
        'views_withheld': withheld,
        'statements': len(statements),
    }
