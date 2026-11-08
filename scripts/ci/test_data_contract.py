"""Every column has to say what it is, and every table who owns it.

This is phase 0 of #231. The manifest already decided which tables exist and
what shape they are; what it never recorded was anything a governance review
asks for -- who is accountable for a table, whether a column identifies a
person, and how stale a store is allowed to be before it should not be used
to answer a question about now.

`customers.email` is the example that makes the case. It has been in the
manifest since the repo was written, marked `unique_columns` because dbt
needed to know, and nothing anywhere recorded that it identifies a human
being. Nobody decided that; there was simply no field to put it in.

A field alone would not have fixed it, which is why this file exists. The
contract is only a contract if a column cannot arrive without one, so the
check is set equality against `columns` rather than a lookup: a new column
that nobody classified fails the build, and so does a classification naming
a column that no longer exists.

The SLA numbers are routing inputs for phase 2, not promises -- nothing
enforces them. They are here because the assistant currently picks an engine
from the words "seconds fresh" in a system prompt, and #188 measured the
mirror at 84 seconds while #225 found it a day stale behind a connector task
that still reported RUNNING. A number in the manifest can at least be
compared against the freshness query sitting next to it.
"""
import pathlib
import sys

REPO = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'airflow' / 'dags'))

import pipeline_config as pc  # noqa: E402

MANIFEST = pc.load_manifest(str(REPO / 'airflow' / 'dags' / 'config' / 'pipelines.yml'))

# phi has no columns in this schema. It is in the vocabulary so that adding a
# health table is a classification, not a change to this file and everything
# that reads it.
VALID = {'public', 'internal', 'pii', 'phi'}


def test_every_table_has_an_owner():
    for name, table in MANIFEST['tables'].items():
        owner = table.get('owner')
        assert owner, f"{name} has no owner"


def test_every_store_declares_a_freshness_sla():
    for name, store in MANIFEST['stores'].items():
        seconds = (store.get('sla') or {}).get('freshness_seconds')
        assert isinstance(seconds, int) and seconds > 0, \
            f"{name} has no usable sla.freshness_seconds"


def test_classifications_use_the_known_vocabulary():
    for name, table in MANIFEST['tables'].items():
        unknown = set(table.get('classification') or {}) - VALID
        assert not unknown, f"{name} uses unknown classifications: {sorted(unknown)}"


def test_every_column_is_classified_exactly_once():
    """Set equality, not a lookup.

    A missing column means someone added one without saying what it is. An
    extra means a classification outlived the column it described, which is
    how a list quietly stops matching the thing it governs.
    """
    for name, table in MANIFEST['tables'].items():
        declared = list(table['columns'])
        classified = [
            col
            for cols in (table.get('classification') or {}).values()
            for col in cols
        ]

        duplicates = {c for c in classified if classified.count(c) > 1}
        assert not duplicates, f"{name}: classified more than once: {sorted(duplicates)}"

        missing = set(declared) - set(classified)
        assert not missing, f"{name}: columns with no classification: {sorted(missing)}"

        extra = set(classified) - set(declared)
        assert not extra, f"{name}: classified but not a column: {sorted(extra)}"


def test_the_sla_can_be_measured_against_something():
    """An SLA is only meaningful if the store can report its own freshness.

    Both halves already live in the manifest; this keeps them together so a
    new store cannot arrive with a target and no way to check it.
    """
    for name, store in MANIFEST['stores'].items():
        assert store.get('freshness'), f"{name} has an SLA but no freshness query"
