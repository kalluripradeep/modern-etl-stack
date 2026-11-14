"""The metrics endpoint is unauthenticated, so what it emits is a security question.

Phase 5 of #231. Prometheus cannot hold a password without that password
living in a ConfigMap next to the scrape config, so `/api/metrics` is exempt
from the dashboard's basic auth. That exemption is only defensible because of
what the route emits: store names, table names and counts.

A metric label is a bad place to discover you put something. These guards
hold the two halves of that bargain -- the exemption stays narrow, and the
route stays incapable of carrying SQL, column values or the question someone
asked.

The live scrape at the bottom runs when a dashboard is reachable and skips
when it is not, so unit CI gains no dependency.
"""
import os
import pathlib
import re

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]
PROXY = (REPO / 'ui' / 'src' / 'proxy.ts').read_text(encoding='utf-8')
ROUTE = (REPO / 'ui' / 'src' / 'app' / 'api' / 'metrics' / 'route.ts').read_text(encoding='utf-8')
METRICS = (REPO / 'ui' / 'src' / 'lib' / 'metrics.ts').read_text(encoding='utf-8')
CHAT = (REPO / 'ui' / 'src' / 'app' / 'api' / 'chat' / 'route.ts').read_text(encoding='utf-8')


def test_only_the_metrics_route_is_exempt_from_auth():
    matcher = re.search(r'matcher: \[(.*?)\]', PROXY, re.S)
    assert matcher, 'the proxy no longer declares a matcher'
    exempt = matcher.group(1)
    assert 'api/metrics' in exempt, \
        'Prometheus cannot scrape a route behind basic auth'
    # Nothing else may be added quietly alongside it.
    for route in ('api/chat', 'api/'):
        assert f'|{route})' not in exempt, f'{route} is exempt from auth'

    # /api/catalog serves table names and the lineage graph (#231 phase 1).
    # The metrics exemption is defensible because Prometheus cannot hold a
    # password and because that route emits only counts; neither is true here,
    # and "it is also read-only" is not the same argument.
    assert 'api/catalog' not in exempt, \
        'the catalog endpoint is exempt from auth; it is not a scrape target'


def test_the_metrics_route_cannot_emit_a_query_or_a_question():
    """It records counts, never the thing that was counted."""
    for forbidden in ('record.queries', 'q.sql', '.question', 'queryLog'):
        assert forbidden not in ROUTE, \
            f'the metrics route references {forbidden}; it must emit counts only'


def _code_only(source):
    """Strip comments and block docstrings.

    Three guards in this repo have now matched their own explanation rather
    than the thing they guard: a file that documents why it must never carry
    SQL necessarily contains the word.
    """
    without_block = re.sub(r'/\*.*?\*/', '', source, flags=re.S)
    return '\n'.join(
        line for line in without_block.split('\n')
        if not line.strip().startswith(('*', '//'))
    )


def test_the_counters_hold_no_free_text():
    """A counter keyed by question text would leak through a label."""
    code = _code_only(METRICS).lower()
    assert 'question' not in code, 'the metric registry stores question text'
    assert 'sql' not in code, 'the metric registry stores SQL'


def test_labels_are_escaped():
    """A quote or newline in a label would break the exposition format, and a
    broken format is how a scrape starts reading the next line as a sample."""
    assert 'escapeLabel' in METRICS
    for char in (r'\\\\', r'\\"'):
        assert char in METRICS, 'label escaping no longer covers quotes and backslashes'


def test_a_denied_query_is_counted_separately_from_a_failure():
    """Counting the access policy as an error makes a governed platform look
    broken, and would hide a real error rate behind it."""
    assert 'classifyError' in CHAT, 'the chat route no longer classifies denials'
    assert "recordQuery(store, 'ok')" in CHAT


def test_an_sla_block_is_recorded():
    assert 'recordSlaBlock' in CHAT, \
        'stores withheld for staleness are no longer counted'


# ── Against a running dashboard ──────────────────────────────────────────────

URL = os.environ.get('METRICS_URL')


@pytest.fixture()
def scraped():
    if not URL:
        pytest.skip('set METRICS_URL to scrape a running dashboard')
    requests = pytest.importorskip('requests')
    try:
        response = requests.get(URL, timeout=5)
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f'no dashboard at {URL}: {exc}')
    assert response.status_code == 200, response.status_code
    return response.text


def test_a_live_scrape_carries_no_sql_and_no_personal_data(scraped):
    # Sample lines only. HELP text is prose written in this repo, not scraped
    # data, and it is allowed to contain the word "from".
    samples = '\n'.join(
        line for line in scraped.split('\n')
        if line and not line.startswith('#')
    ).lower()
    for marker in ('select ', ' from ', 'where ', '@', 'insert '):
        assert marker not in samples, f'a sample line contains {marker!r}'


def test_a_live_scrape_is_valid_exposition_format(scraped):
    for line in scraped.split('\n'):
        if not line or line.startswith('#'):
            continue
        assert re.match(r'^[a-z_]+(\{[^}]*\})? -?[0-9.e+]+$', line), line
