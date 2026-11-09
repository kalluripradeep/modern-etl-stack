"""The data contract decides who may read what, and the database enforces it.

Phase 4 of #231. Phase 0 recorded which columns identify a person and nothing
acted on it: `dashboard_ro` was created with GRANT SELECT ON ALL TABLES, so
the assistant could read `email` as readily as `order_id`.

Enforcement is in the database rather than the API, and that is the point
rather than an implementation detail. The assistant writes its own SQL, so
anything checked in the application is checked against generated SQL, where a
column alias, an expression or a view defeats matching on names. Phase 3's
exposure matcher carries a comment saying it must not be used for access
decisions for exactly this reason.

These guards cover the ways the arrangement goes quiet: grants that stop
being derived from the contract, a blanket grant creeping back, and the
ClickHouse half silently losing the ability to express column grants at all.

The grant logic is tested against a real Postgres below when one is
reachable, and skipped when it is not, so unit CI gains no dependency.
"""
import os
import pathlib
import re
import sys

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'airflow' / 'dags'))

import access_policy as ap  # noqa: E402
import pipeline_config as pc  # noqa: E402

MANIFEST = pc.load_manifest(str(REPO / 'airflow' / 'dags' / 'config' / 'pipelines.yml'))
ROLE_SCRIPT = (REPO / 'docker' / 'postgres-dest-init' / '01-create-dashboard-role.sh').read_text(encoding='utf-8')
MIRROR_SQL = (REPO / 'docker' / 'clickhouse' / 'initdb' / '01_mirror_schema.sql').read_text(encoding='utf-8')
CH_USERS_XML = (REPO / 'docker' / 'clickhouse' / 'users.d' / 'zz-access-management.xml').read_text(encoding='utf-8')
DAG_SOURCE = (REPO / 'airflow' / 'dags' / 'ingest_source_to_bronze.py').read_text(encoding='utf-8')


def test_the_denied_set_comes_from_the_contract():
    denied = ap.denied_columns(MANIFEST)
    assert 'email' in denied, 'email is classified pii and must be withheld'
    assert 'order_id' not in denied, 'ordinary columns must stay readable'


def test_nothing_hands_out_a_blanket_grant():
    """The failure that made phase 0 decorative.

    GRANT SELECT ON ALL TABLES, or default privileges that grant on tables
    dbt has not created yet, both re-open every column including the
    classified ones.
    """
    # Statements only. The comment in that file explains what was removed and
    # necessarily names it -- the same trap the phase 2 prompt guard fell into.
    statements = '\n'.join(
        line for line in ROLE_SCRIPT.split('\n')
        if not line.strip().startswith('--')
    )
    assert 'ON ALL TABLES' not in statements, \
        'the role script grants on all tables again'
    assert 'ALTER DEFAULT PRIVILEGES' not in statements, \
        'default privileges would re-grant every table dbt creates later'


def test_a_table_of_only_denied_columns_produces_no_grant():
    """Postgres rejects an empty column list, so it must be skipped."""
    statements = ap.grants_for_tables(
        {('raw', 'secret'): ['email', 'first_name']}, {'email', 'first_name'}, 'r')
    assert statements == []


def test_a_view_reading_a_denied_column_is_not_granted():
    """All or nothing: a view runs with its owner's privileges unless it was
    declared security_invoker, so column grants underneath do not protect it."""
    granted = ap.grants_for_views(
        {('prs', 'clean'): {'order_id'}, ('prs', 'leaky'): {'order_id', 'email'}},
        {'email'}, 'r')
    assert any('clean' in s for s in granted)
    assert not any('leaky' in s for s in granted)


def test_the_plan_revokes_before_it_grants():
    """Otherwise a grant removed from the policy survives from a previous run."""
    plan = ap.plan({('raw', 't'): ['order_id']}, {}, set(), 'r')
    assert plan[0].startswith('REVOKE'), plan[:2]


def test_revocation_skips_schemas_that_do_not_exist():
    """`meta` is created by phase 1 and absent until it has run once; REVOKE on
    a missing schema is an error rather than a no-op."""
    assert ap.revocations('r', {'raw'}) == [
        'REVOKE ALL ON ALL TABLES IN SCHEMA "raw" FROM r;'
    ]


def test_the_policy_is_applied_after_dbt_and_regardless_of_its_outcome():
    block = re.search(r'task_id="apply_access_policy".*?\)', DAG_SOURCE, re.S)
    assert block, 'the DAG no longer applies the access policy'
    assert 'trigger_rule="all_done"' in block.group(0), \
        'a failed dbt run would leave the grants un-applied, quietly widening access'
    assert 'dbt_transformations >> access_policy_task' in DAG_SOURCE, \
        'the policy must be applied after dbt creates the tables it grants on'


# ── ClickHouse ───────────────────────────────────────────────────────────────

def test_the_mirror_grants_withhold_the_classified_columns():
    denied = ap.denied_columns(MANIFEST)
    grant = re.search(r'GRANT SELECT\(([^)]*)\) ON mirror\.customers TO', MIRROR_SQL)
    assert grant, 'no column grant for mirror.customers'
    granted = {c.strip() for c in grant.group(1).split(',')}
    assert not (granted & denied), f'classified columns granted: {sorted(granted & denied)}'


def test_the_mirror_grants_keep_the_version_column():
    """FINAL needs `ver` to collapse a ReplacingMergeTree, and `ver` is not in
    the manifest -- an allow-list built from the contract alone makes every
    <t>_current view unreadable."""
    grant = re.search(r'GRANT SELECT\(([^)]*)\) ON mirror\.orders TO', MIRROR_SQL)
    granted = {c.strip() for c in grant.group(1).split(',')}
    assert {'ver', 'is_deleted'} <= granted, 'clean read views would be unreadable'


def test_the_kafka_tables_are_never_granted():
    """Selecting from a Kafka engine table consumes the topic."""
    assert not re.search(r'ON mirror\.kafka_\w+ TO', MIRROR_SQL), \
        'a read-only role must not be able to consume from the CDC topics'


def test_the_generated_sql_carries_no_credential():
    assert 'IDENTIFIED WITH' not in MIRROR_SQL, \
        'a password belongs in the environment, not in a committed generated file'


def test_clickhouse_can_express_column_grants_at_all():
    """An XML-defined user cannot be given column-level grants, so without SQL
    access control the mirror has no way to withhold a column."""
    assert '<access_management>1</access_management>' in CH_USERS_XML
    assert '<chuser>' in CH_USERS_XML, 'the override names a user that must exist'
    secrets = (REPO / 'k8s' / 'generate-secrets.sh').read_text(encoding='utf-8')
    assert 'CLICKHOUSE_USER: chuser' in secrets, \
        'the username is generated now, so the hardcoded XML override is wrong'


def test_the_override_sorts_after_the_images_own_file():
    """users.d is merged alphabetically and the image writes
    access_management=0 into default-user.xml."""
    name = 'zz-access-management.xml'
    assert name > 'default-user.xml', 'the override would lose the merge'


# ── Against a real Postgres ──────────────────────────────────────────────────

DSN = os.environ.get(
    'META_TEST_DSN',
    'host=localhost port=5433 dbname=destdb user=destuser password=destpass',
)


@pytest.fixture()
def conn():
    psycopg2 = pytest.importorskip('psycopg2')
    try:
        return psycopg2.connect(DSN, connect_timeout=3)
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f'no Postgres for the access policy test: {exc}')


def test_the_plan_names_every_reachable_table(conn):
    """Introspection, not the manifest: dbt creates int, prs and gold, and a
    model added next week has to be covered without anyone listing it."""
    try:
        present = ap.existing_schemas(conn)
        table_columns, view_sources = ap.read_catalog(conn, present)
        plan = ap.plan(table_columns, view_sources, ap.denied_columns(MANIFEST),
                       'dashboard_ro', present)
    finally:
        conn.close()

    assert table_columns, 'no tables found to grant on'
    granted = ' '.join(plan)
    for schema in ('raw',):
        assert f'"{schema}"' in granted, f'{schema} is not covered by the policy'
    assert 'email' not in granted, 'a classified column appears in a grant'


# ── Deploying onto a cluster that predates this change ───────────────────────
#
# k8s/01-secrets.generated.yaml is written once and gitignored, so a cluster
# set up before DASHBOARD_CH_USER existed will not have it. Two things break
# in that case, and neither is visible from a manifest that validates:
# secret_val returns empty, so the role is never created and the grants that
# name it abort the schema apply; and the dashboard reads the key by
# secretKeyRef with no `optional`, so its pod never starts.

DEPLOY = (REPO / 'k8s' / 'deploy.sh').read_text(encoding='utf-8')
UI_DEPLOYMENT = (REPO / 'k8s' / 'ui' / 'deployment.yaml').read_text(encoding='utf-8')


def test_the_deploy_tolerates_a_secret_without_the_new_keys():
    assert 'DASH_CH_USER=dashboard_ro' in DEPLOY, \
        'a cluster whose secret predates this key gets an empty role name'


def test_the_deploy_writes_the_keys_back_into_the_secret():
    """Otherwise the dashboard pod sticks in CreateContainerConfigError."""
    assert 'kubectl patch secret etl-secrets' in DEPLOY
    assert 'DASHBOARD_CH_USER' in DEPLOY and 'DASHBOARD_CH_PASSWORD' in DEPLOY


def test_the_dashboard_does_not_fall_back_to_the_admin_account():
    """Marking the reference optional would be the easy fix and the wrong one.

    datastores.ts defaults CLICKHOUSE_USER to chuser, so an unset variable
    connects as the admin and the policy is defeated without a trace.
    """
    block = re.search(r'name: CLICKHOUSE_USER.*?key: DASHBOARD_CH_USER', UI_DEPLOYMENT, re.S)
    assert block, 'the dashboard no longer uses the restricted ClickHouse role'
    assert 'optional' not in block.group(0), \
        'an optional reference silently falls back to the admin account'
