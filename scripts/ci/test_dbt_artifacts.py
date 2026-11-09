"""dbt already knows which tables are tested. The scoring must not lie about it.

Phase 1 of #231. `dbt test` writes target/run_results.json and every dbt run
rewrites target/manifest.json; both were discarded at the end of the task.
Neither is useful alone -- run_results knows which tests passed and nothing
about what they tested, the manifest knows a test depends on
`model.modern_etl.fact_order_items` and where that model lands. The join is
the whole job, and it is what these tests cover.

The property worth guarding hardest is the untested one. `passed / total`
gives a table nobody tested a score of 1.0, which displays as "verified" and
means "nobody looked" -- the single most dangerous number this code could
produce, because a governance layer is read by people deciding whether to
trust a figure. Untested stays NULL, all the way through to the column type.

Run results are synthesised from the real manifest's test ids. The parsing is
against the genuine artifact; only pass/fail outcomes are chosen, because a
real run_results.json requires a built warehouse and the shape of an entry
(status, unique_id, failures) is already confirmed against the checked-in
file.
"""
import json
import subprocess
import re
import pathlib
import sys

REPO = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'airflow' / 'dags'))

import dbt_artifacts as da  # noqa: E402

TARGET = REPO / 'dbt' / 'target'
MANIFEST = json.loads((TARGET / 'manifest.json').read_text(encoding='utf-8'))
TEST_IDS = [k for k, v in MANIFEST['nodes'].items() if v['resource_type'] == 'test']


def results_for(statuses):
    """A run_results payload in dbt's shape, for the manifest's real tests."""
    return {
        'metadata': {'generated_at': '2026-11-08T10:00:00.000000Z'},
        'results': [
            {'unique_id': tid, 'status': statuses.get(tid, 'pass'),
             'failures': 0 if statuses.get(tid, 'pass') == 'pass' else 1}
            for tid in TEST_IDS
        ],
    }


def test_the_manifest_has_tests_to_join_against():
    """If this fails the rest of the file is vacuously green."""
    assert len(TEST_IDS) > 0, 'no tests in the dbt manifest'


def test_an_untested_table_scores_null_not_one():
    rows = {r['table_name']: r for r in da.quality(MANIFEST, results_for({}))}
    untested = [r for r in rows.values() if r['tests_total'] == 0]
    assert untested, 'expected at least one model with no tests'
    for row in untested:
        assert row['score'] is None, \
            f"{row['table_name']} has no tests but scored {row['score']}"


def test_a_passing_table_scores_one():
    rows = {r['table_name']: r for r in da.quality(MANIFEST, results_for({}))}
    tested = [r for r in rows.values() if r['tests_total'] > 0]
    assert tested, 'expected at least one tested model'
    for row in tested:
        assert row['score'] == 1.0, f"{row['table_name']} scored {row['score']} with all tests passing"


def test_a_failure_lowers_the_score_of_the_table_it_tested():
    """And only that table -- a shared failure must not smear across models."""
    victim = TEST_IDS[0]
    owners = [
        da._index(MANIFEST).get(p)
        for p in MANIFEST['nodes'][victim]['depends_on']['nodes']
    ]
    owners = [o for o in owners if o]
    rows = {r['table_name']: r for r in da.quality(MANIFEST, results_for({victim: 'fail'}))}

    for name in owners:
        assert rows[name]['score'] < 1.0, f"{name} should have dropped below 1.0"
        assert rows[name]['failures'] >= 1

    for name, row in rows.items():
        if name not in owners and row['tests_total']:
            assert row['score'] == 1.0, f"{name} was dragged down by another table's failure"


def test_scores_stay_within_range():
    rows = da.quality(MANIFEST, results_for({TEST_IDS[0]: 'fail'}))
    for row in rows:
        assert row['tests_passed'] <= row['tests_total']
        assert row['score'] is None or 0.0 <= row['score'] <= 1.0


def test_lineage_edges_are_tables_at_both_ends():
    """dbt's parent_map includes tests, which have parents but are not tables."""
    index = set(da._index(MANIFEST).values())
    edges = da.lineage(MANIFEST)
    assert edges, 'expected lineage edges'
    for edge in edges:
        assert edge['child'] in index, edge
        assert edge['parent'] in index, edge
        assert edge['child'] != edge['parent'], f"self-edge: {edge}"


def test_lineage_reaches_the_sources():
    """A gold table has to trace back to raw, or the graph is not lineage."""
    edges = da.lineage(MANIFEST)
    parents = {}
    for edge in edges:
        parents.setdefault(edge['child'], []).append(edge['parent'])

    seen, stack = set(), ['gold.fact_order_items']
    while stack:
        node = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        stack.extend(parents.get(node, []))

    assert any(n.startswith('raw.') for n in seen), \
        f"gold.fact_order_items does not trace back to raw: {sorted(seen)}"


def test_missing_run_results_is_not_a_crash():
    """A first run, or a dbt invocation that ran no tests."""
    rows = da.quality(MANIFEST, {'results': []})
    assert rows, 'models should still be listed'
    assert all(r['score'] is None for r in rows), 'nothing ran, so nothing is scored'


# ── The SQL, against a real database ─────────────────────────────────────────
#
# Everything above parses JSON. None of it touches the statements that
# actually ship, and a parameter-binding mistake there fails only in Airflow,
# at the end of a pipeline run, where nobody is watching. These run when a
# Postgres is reachable and skip when it is not, so they cover the real
# statements locally and during deploy smoke without making unit CI need a
# database.

import os  # noqa: E402

import pytest  # noqa: E402

DSN = os.environ.get(
    'META_TEST_DSN',
    'host=localhost port=5433 dbname=destdb user=destuser password=destpass',
)


@pytest.fixture()
def conn():
    psycopg2 = pytest.importorskip('psycopg2')
    try:
        connection = psycopg2.connect(DSN, connect_timeout=3)
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f'no Postgres for the meta schema test: {exc}')
    with connection.cursor() as cur:
        cur.execute('DROP SCHEMA IF EXISTS meta CASCADE')
    connection.commit()
    yield connection
    connection.close()


def _target(tmp_path, statuses):
    (tmp_path / 'manifest.json').write_text(
        json.dumps(MANIFEST), encoding='utf-8')
    (tmp_path / 'run_results.json').write_text(
        json.dumps(results_for(statuses)), encoding='utf-8')
    return tmp_path


def test_persist_creates_the_schema_and_writes_both_tables(conn, tmp_path):
    summary = da.persist(conn, _target(tmp_path, {}))
    assert summary['tables'] > 0 and summary['edges'] > 0

    with conn.cursor() as cur:
        cur.execute('SELECT count(*) FROM meta.dbt_quality')
        assert cur.fetchone()[0] == summary['tables']
        cur.execute('SELECT count(*) FROM meta.dbt_lineage')
        assert cur.fetchone()[0] == summary['edges']

        # The NULL has to survive the round trip through NUMERIC(5,4). A
        # driver or column type that coerced it to 0 would turn "untested"
        # into "failed everything", which is a different lie.
        cur.execute('SELECT score FROM meta.dbt_quality WHERE tests_total = 0')
        scores = [r[0] for r in cur.fetchall()]
        assert scores and all(s is None for s in scores)


def test_persist_is_idempotent(conn, tmp_path):
    first = da.persist(conn, _target(tmp_path, {}))
    second = da.persist(conn, _target(tmp_path, {}))
    assert first == second

    with conn.cursor() as cur:
        cur.execute('SELECT count(*) FROM meta.dbt_quality')
        assert cur.fetchone()[0] == first['tables'], 'rows duplicated on re-run'
        cur.execute('SELECT count(*) FROM meta.dbt_lineage')
        assert cur.fetchone()[0] == first['edges']


def test_a_later_run_overwrites_an_earlier_score(conn, tmp_path):
    victim = TEST_IDS[0]
    owner = da._index(MANIFEST)[MANIFEST['nodes'][victim]['depends_on']['nodes'][0]]

    da.persist(conn, _target(tmp_path, {victim: 'fail'}))
    with conn.cursor() as cur:
        cur.execute('SELECT score FROM meta.dbt_quality WHERE table_name = %s', (owner,))
        failed = cur.fetchone()[0]
    assert failed < 1

    da.persist(conn, _target(tmp_path, {}))
    with conn.cursor() as cur:
        cur.execute('SELECT score FROM meta.dbt_quality WHERE table_name = %s', (owner,))
        assert cur.fetchone()[0] == 1, 'a fixed table kept its old failing score'


def test_no_manifest_is_not_a_failure(conn, tmp_path):
    assert da.persist(conn, tmp_path / 'does-not-exist') is None


# ── The task must produce its own artifacts ──────────────────────────────────
#
# The first version of persist_dbt_metadata read DBT_PROJECT_PATH/target. In a
# deployed stack that directory does not exist: Cosmos clones the project into
# a tempfile.TemporaryDirectory() and runs dbt there, so nothing is written
# back, and dbt/target/ is gitignored, so on Kubernetes -- where the DAGs
# arrive by git-sync -- it is absent entirely. The task logged "no manifest"
# and returned, which looks identical to "nothing to do".
#
# It worked on a developer machine only because stale artifacts happened to be
# lying about, which is the worst way for a thing to appear to work.

DAG_SOURCE = (REPO / 'airflow' / 'dags' / 'ingest_source_to_bronze.py').read_text(encoding='utf-8')


def test_dbt_target_is_not_committed():
    """If this ever changes, the reasoning below needs revisiting."""
    tracked = subprocess.run(
        ['git', 'ls-files', 'dbt/target'],
        cwd=REPO, capture_output=True, text=True,
    ).stdout.strip()
    assert not tracked, (
        'dbt/target is tracked now; the task could read it directly again, '
        'but only if Cosmos also stopped running dbt in a temporary directory'
    )


def test_the_task_does_not_read_the_projects_target_directory():
    assert 'DBT_PROJECT_PATH / "target"' not in DAG_SOURCE, (
        'persist_dbt_metadata reads the project target directory again; it is '
        'empty under Cosmos and absent on Kubernetes'
    )
    assert '"--target-path"' in DAG_SOURCE, \
        'the task no longer directs dbt to a target path it controls'


def test_the_task_does_not_write_logs_into_the_project_directory():
    """#225: dbt writes logs/dbt.log into --project-dir unless redirected.

    On Kubernetes the project is a read-only git-sync checkout, so dbt dies
    before its own logger exists: exit 2, nothing on stdout, nothing on
    stderr. Directing --target-path elsewhere is not enough, because the log
    path is separate and defaults into the project.
    """
    assert 'DBT_LOG_PATH' in DAG_SOURCE, (
        'persist_dbt_metadata no longer redirects the dbt log path; on a '
        'read-only project directory dbt exits 2 with no output at all'
    )


def test_failing_tests_do_not_fail_the_capture():
    """A failing test is the finding. Exit 1 must be accepted."""
    assert 'returncode not in (0, 1)' in DAG_SOURCE, \
        'the task no longer treats "some tests failed" as data'


def test_the_capture_runs_even_when_an_upstream_dbt_test_failed():
    """Otherwise it skips precisely when there is something to record."""
    block = re.search(r'task_id="persist_dbt_metadata".*?\)', DAG_SOURCE, re.S)
    assert block and 'trigger_rule="all_done"' in block.group(0), \
        'persist_dbt_metadata would be skipped when a dbt test task fails'


def test_an_absent_manifest_is_loud():
    """Silence is what hid the original bug."""
    assert re.search(r'if summary is None:.*?raise AirflowException', DAG_SOURCE, re.S), \
        'a missing manifest is logged and swallowed again'
