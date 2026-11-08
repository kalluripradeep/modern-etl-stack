"""Turn dbt's own run artifacts into quality scores and lineage.

Phase 1 of #231. Nothing new runs here. `dbt test` already writes
`target/run_results.json` and every invocation rewrites `target/manifest.json`,
and both were being thrown away at the end of each run. The platform had 58
passing tests and no way to answer "is this table trustworthy", which is the
first question a governance review asks and the one phase 2 has to score a
store on.

The join is the whole job. `run_results.json` knows which tests passed and
nothing about what they tested; `manifest.json` knows that a test depends on
`model.modern_etl.fact_order_items` and that the model lands at
`"destdb"."gold"."fact_order_items"`. Neither file is useful alone.

A deliberate choice worth stating, because it is the one that makes this
honest: **a table with no tests scores NULL, not 1.0.** Dividing passes by
tests gives an untested table a perfect score, which is the most dangerous
number this file could produce -- it reads as "verified" and means "nobody
looked". Untested and clean are different states and the schema keeps them
different.
"""
import json
import pathlib
from datetime import datetime, timezone


def _relation(node):
    """Normalise a dbt node to `schema.name`.

    `relation_name` is quoted and database-qualified ('"destdb"."gold"."x"'),
    which is correct for dbt and wrong for everything else -- the assistant,
    the store registry and the manifest all speak `schema.table`.
    """
    return f"{node['schema']}.{node['name']}"


def _index(manifest):
    """node_id -> schema.name, across both models and sources."""
    index = {}
    for node_id, node in manifest.get('nodes', {}).items():
        if node.get('resource_type') == 'model':
            index[node_id] = _relation(node)
    for node_id, node in manifest.get('sources', {}).items():
        index[node_id] = _relation(node)
    return index


def quality(manifest, run_results):
    """Per-table test outcomes from one dbt run.

    Returns rows of (table, tests_total, tests_passed, failures, score),
    where score is None when the table has no tests at all.
    """
    index = _index(manifest)
    nodes = manifest.get('nodes', {})

    # Every model starts at zero tests. A model that is dropped from the test
    # suite should appear as untested rather than silently keep the score from
    # whenever it was last covered.
    tally = {name: {'total': 0, 'passed': 0, 'failures': 0} for name in index.values()}

    for result in run_results.get('results', []):
        node = nodes.get(result['unique_id'])
        if not node or node.get('resource_type') != 'test':
            continue  # models, seeds, operations -- this function is about tests
        for parent in node.get('depends_on', {}).get('nodes', []):
            table = index.get(parent)
            if table is None:
                continue
            entry = tally[table]
            entry['total'] += 1
            # dbt reports 'pass' for tests; 'warn' is a threshold not reached,
            # which is not a failure but is not a clean pass either.
            if result['status'] == 'pass':
                entry['passed'] += 1
            entry['failures'] += int(result.get('failures') or 0)

    rows = []
    for table in sorted(tally):
        entry = tally[table]
        score = round(entry['passed'] / entry['total'], 4) if entry['total'] else None
        rows.append({
            'table_name': table,
            'tests_total': entry['total'],
            'tests_passed': entry['passed'],
            'failures': entry['failures'],
            'score': score,
        })
    return rows


def lineage(manifest):
    """Parent/child edges between tables, in `schema.name` terms.

    dbt's parent_map includes tests, which have a parent but are not a table.
    Both ends have to resolve to something queryable or the edge is noise.
    """
    index = _index(manifest)
    edges = set()
    for child_id, parent_ids in manifest.get('parent_map', {}).items():
        child = index.get(child_id)
        if child is None:
            continue
        for parent_id in parent_ids:
            parent = index.get(parent_id)
            if parent is not None and parent != child:
                edges.add((child, parent))
    return [{'child': c, 'parent': p} for c, p in sorted(edges)]


def run_timestamp(run_results):
    """When the run that produced these results finished."""
    stamp = run_results.get('metadata', {}).get('generated_at')
    if not stamp:
        return datetime.now(timezone.utc)
    return datetime.fromisoformat(stamp.replace('Z', '+00:00'))


def load(target_dir):
    target = pathlib.Path(target_dir)
    manifest = json.loads((target / 'manifest.json').read_text(encoding='utf-8'))
    results_path = target / 'run_results.json'
    results = json.loads(results_path.read_text(encoding='utf-8')) if results_path.exists() else {'results': []}
    return manifest, results
