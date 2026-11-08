"""The routing policy has to be data, and it has to reach the consumer intact.

Phase 2 of #231. The assistant used to choose a store from two adjectives in
its system prompt -- "seconds fresh" for the mirror, "batch, daily fresh" for
the warehouse. #188 measured the mirror at 84 seconds against a 30 second
target and #225 found it a day behind a connector task that was still
reporting RUNNING. Both read as "seconds fresh" to the old prompt.

The decision now rests on a measurement and a policy. The policy is in the
manifest so it is reviewable in a pull request rather than buried in prose,
and these guards cover the two ways that arrangement fails quietly: a policy
that cannot be enforced, and a policy that never reaches TypeScript.

The scoring itself is tested in ui/src/lib/routing.test.ts, which runs under
`node --test` with no database and no API key.
"""
import json
import pathlib
import re
import sys

REPO = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'airflow' / 'dags'))

import pipeline_config as pc  # noqa: E402

MANIFEST = pc.load_manifest(str(REPO / 'airflow' / 'dags' / 'config' / 'pipelines.yml'))
POLICY = MANIFEST['routing_policy']
GENERATED = (REPO / 'ui' / 'src' / 'lib' / 'stores.generated.ts').read_text(encoding='utf-8')


def test_the_policy_exists_and_is_complete():
    for key in ('probe', 'stale_excludes_store', 'quality_warn_below',
                'measurement_ttl_seconds'):
        assert key in POLICY, f'routing_policy is missing {key}'


def test_the_probe_names_a_real_table_and_column():
    probe = POLICY['probe']
    table = MANIFEST['tables'].get(probe['table'])
    assert table, f"probe table {probe['table']} is not in the manifest"
    assert probe['column'] in table['columns'], \
        f"probe column {probe['column']} is not a column of {probe['table']}"


def test_every_store_can_be_probed():
    """A store with an SLA and no way to measure it has an unenforceable SLA."""
    probe = POLICY['probe']
    for name, store in MANIFEST['stores'].items():
        sql = store['freshness'].format(
            table=pc.store_table(MANIFEST, name, probe['table']),
            col=probe['column'],
        )
        assert '{' not in sql, f'{name}: freshness still has an unfilled placeholder: {sql}'
        assert sql.lower().lstrip().startswith('select'), f'{name}: probe is not a SELECT'


def test_the_measurement_ttl_can_judge_the_tightest_sla():
    """A reading older than the tightest SLA cannot decide whether it is met.

    The mirror allows 30 seconds. Caching a reading for longer than that would
    let a store that went stale inside the window keep being offered, which is
    the failure this phase exists to remove rather than relocate.
    """
    tightest = min(s['sla']['freshness_seconds'] for s in MANIFEST['stores'].values())
    assert POLICY['measurement_ttl_seconds'] < tightest, (
        f"measurement_ttl_seconds={POLICY['measurement_ttl_seconds']} is not "
        f'inside the tightest SLA ({tightest}s)'
    )


def test_the_quality_threshold_is_a_fraction():
    warn = POLICY['quality_warn_below']
    assert 0 < warn <= 1, 'quality_warn_below is compared against a 0..1 dbt score'


def test_the_policy_reaches_typescript_unchanged():
    """The generated registry is what the assistant actually reads."""
    block = re.search(r'export const ROUTING_POLICY = \{(.*?)\};', GENERATED, re.S)
    assert block, 'ROUTING_POLICY is not in stores.generated.ts'
    body = block.group(1)

    expected = 'true' if POLICY['stale_excludes_store'] else 'false'
    assert f'staleExcludesStore: {expected},' in body
    assert f"qualityWarnBelow: {POLICY['quality_warn_below']}," in body
    assert f"measurementTtlSeconds: {POLICY['measurement_ttl_seconds']}," in body


def test_every_store_carries_its_sla_and_probe_into_typescript():
    for name, store in MANIFEST['stores'].items():
        assert f"slaFreshnessSeconds: {store['sla']['freshness_seconds']}," in GENERATED, \
            f'{name} SLA did not reach the registry'

    probe = POLICY['probe']
    for name, store in MANIFEST['stores'].items():
        sql = store['freshness'].format(
            table=pc.store_table(MANIFEST, name, probe['table']),
            col=probe['column'],
        )
        # json.dumps because the ClickHouse expression contains its own single
        # quotes -- dateDiff('second', ...) -- and an earlier hand-rolled
        # escape emitted a string that ended early and left the rest as syntax.
        assert f'freshnessSql: {json.dumps(sql)},' in GENERATED, \
            f'{name} probe SQL did not reach the registry intact'


def test_the_prompt_no_longer_claims_a_freshness_it_has_not_measured():
    """The adjectives are the bug. They must not creep back in."""
    route = (REPO / 'ui' / 'src' / 'app' / 'api' / 'chat' / 'route.ts').read_text(encoding='utf-8')

    # The prompt itself, not the file: a comment explaining why the adjectives
    # were removed necessarily quotes them.
    prompt = re.search(r'const SYSTEM_PROMPT = `(.*?)`;', route, re.S)
    assert prompt, 'SYSTEM_PROMPT not found in route.ts'

    overview = (REPO / 'ui' / 'src' / 'lib' / 'datastores.ts').read_text(encoding='utf-8')
    claims = ('seconds fresh', 'daily fresh', 'batch fresh')
    for where, text in (('the system prompt', prompt.group(1)),
                        ('the schema overview', overview)):
        for claim in claims:
            assert claim not in text, (
                f'{where} still asserts {claim!r}; freshness is measured per '
                'request now, and a static claim beside it is the bug this '
                'phase removed'
            )

    assert 'measureStores' in route, 'route.ts does not measure store health'
