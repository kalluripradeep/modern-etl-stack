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
