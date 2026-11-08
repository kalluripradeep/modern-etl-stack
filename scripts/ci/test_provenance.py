"""An audit trail the audited component can edit is not an audit trail.

Phase 3 of #231. The assistant is already asked to name the store it used,
and that is a claim -- generated text that happens to be about the query. The
record is taken from the tool calls that executed instead, and these guards
hold the three structural properties that make it worth keeping.

The assembly itself is tested in ui/src/lib/provenance.test.ts under
`node --test`: table matching, column exposure, and the summary line. What
cannot be covered there is whether route.ts still *uses* it, which is what
this file is for.
"""
import pathlib
import re

REPO = pathlib.Path(__file__).resolve().parents[2]
ROUTE = (REPO / 'ui' / 'src' / 'app' / 'api' / 'chat' / 'route.ts').read_text(encoding='utf-8')
PROV = (REPO / 'ui' / 'src' / 'lib' / 'provenance.ts').read_text(encoding='utf-8')
DATASTORES = (REPO / 'ui' / 'src' / 'lib' / 'datastores.ts').read_text(encoding='utf-8')


def test_the_record_is_emitted_somewhere_durable():
    """One structured line, so a log pipeline can collect it."""
    assert "console.log(JSON.stringify({ event: 'ai_answer'" in ROUTE, \
        'route.ts no longer emits the audit line'


def test_the_assistant_cannot_write_to_the_database():
    """The point of logging rather than persisting.

    The warehouse connection is read-only by design. If provenance were ever
    written through it, the component being audited would own its own
    history -- and the read-only guard would have to be relaxed to allow it,
    which is the change this test is really watching for.
    """
    assert 'default_transaction_read_only=on' in DATASTORES, \
        'the warehouse connection is no longer read-only'
    for forbidden in ('INSERT INTO meta.ai_provenance', 'ai_provenance'):
        assert forbidden not in ROUTE, \
            f'route.ts writes provenance to the database ({forbidden}); it should log it'


def test_every_reply_path_records():
    """A trail with only the successes describes a different session.

    The three paths out of the handler once a key is present: an answer, a
    refusal, and a thrown error. All three ran queries that really ran.
    """
    assert ROUTE.count('recordProvenance(') >= 4, (
        'not every reply path records provenance -- expected the definition '
        'plus the answer, refusal and error paths'
    )
    # The refusal and error branches specifically, since those are the ones
    # that get forgotten.
    refusal = re.search(r"stop_reason === 'refusal'\) \{(.{0,200})", ROUTE, re.S)
    assert refusal and 'recordProvenance(' in refusal.group(1), \
        'the refusal path does not record what ran before it'
    caught = re.search(r'\} catch \(err\) \{\s*console\.error\(err\);(.{0,300})', ROUTE, re.S)
    assert caught and 'recordProvenance(' in caught.group(1), \
        'the error path does not record what ran before it'


def test_a_failed_query_is_still_recorded():
    """The queries that failed are part of what happened."""
    assert re.search(r'log\.push\(\{[^}]*error', ROUTE, re.S), \
        'executeTool discards failed queries instead of recording them'


def test_the_record_comes_from_the_tool_calls_not_the_prose():
    """The whole premise. If this is ever built from the model's text instead,
    it stops being evidence and becomes a second opinion from the suspect."""
    assert 'async function executeTool(' in ROUTE
    assert re.search(r'executeTool\(\s*name: string,\s*input: unknown,\s*log: QueryRecord\[\]',
                     ROUTE), \
        'executeTool no longer receives the query log'
    assert 'TOOL_STORES' in ROUTE, \
        'the store is no longer derived from the tool that was called'


def test_column_exposure_over_reports_rather_than_under_reports():
    """Documented as an approximation, and deliberately the safe direction.

    Substring matching can claim a table named inside a string literal. For
    "did this answer touch personal data" a false positive is a cheap second
    look and a false negative is a missed disclosure -- which is also why the
    docstring has to keep saying it does not drive masking.
    """
    assert 'not a substitute for column-level lineage' in PROV, \
        'the approximation is no longer disclosed where someone would read it'
    assert 'would not be if it drove masking' in PROV, \
        'the limit on what this may be used for is no longer stated'
