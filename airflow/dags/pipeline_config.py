"""Loader for the pipeline manifest (airflow/dags/config/pipelines.yml).

The manifest is the single source of truth for which tables flow through
the platform. The ingestion DAG imports this module directly; other
consumers (ClickHouse schema, Debezium connector, CDC daemon) are
generated from it by scripts/generate_pipeline_assets.py.
"""

import os
import re
from pathlib import Path

import yaml

# PostgreSQL type -> ClickHouse type (Debezium JSON with
# decimal.handling.mode=double, temporal types in adaptive precision:
# TIMESTAMP as epoch micros, DATE as epoch days).
# NUMERIC is handled separately (Decimal64 with the declared scale).
_PG_TO_CLICKHOUSE = {
    'BIGINT': 'Int64',
    'INTEGER': 'Int32',
    'SMALLINT': 'Int16',
    'BOOLEAN': 'Bool',
    'REAL': 'Float32',
    'DOUBLE PRECISION': 'Float64',
    'DATE': 'Date',
    'TEXT': 'String',
    'VARCHAR': 'String',
    'CHAR': 'String',
    'UUID': 'String',       # stored as String — avoids toUUID parse errors on nulls
    'JSON': 'String',
    'JSONB': 'String',
    'TIMESTAMP': 'DateTime64(6)',
}


def load_manifest(path=None):
    """Load and validate the manifest. Returns the parsed dict."""
    if path is None:
        path = os.environ.get(
            'PIPELINES_CONFIG',
            str(Path(__file__).parent / 'config' / 'pipelines.yml'),
        )
    with open(path, encoding='utf-8') as fh:
        manifest = yaml.safe_load(fh)

    for name, table in manifest['tables'].items():
        if table['primary_key'] not in table['columns']:
            raise ValueError(f"{name}: primary_key {table['primary_key']} not in columns")
        for key in ('update_columns', 'low_cardinality'):
            missing = [c for c in table.get(key, []) if c not in table['columns']]
            if missing:
                raise ValueError(f"{name}: {key} not in columns: {missing}")
        _validate_partition(name, table)
    return manifest


def _validate_partition(name, table):
    """A ReplacingMergeTree only collapses duplicates within one partition.

    Every CDC event appends a row, and the versions of a key are only reduced
    to one when the parts holding them are merged -- which happens per
    partition. Partition on a column the source can UPDATE and the versions of
    a single key land in different partitions, where no merge will ever bring
    them together. They stop being duplicates ClickHouse can collapse and
    become permanent copies, and `FINAL` will not save you either: it dedupes
    per partition too, so `<t>_current` starts returning one row per partition
    the key ever visited.

    That failure is silent and unbounded, so the column is checked here rather
    than left to whoever edits the manifest next.
    """
    col = table.get('partition_column')
    if col is None:
        return
    if col not in table['columns']:
        raise ValueError(f"{name}: partition_column {col} not in columns")
    base = table['columns'][col].upper()
    if not (base.startswith('TIMESTAMP') or base.startswith('DATE')):
        raise ValueError(
            f"{name}: partition_column {col} is {table['columns'][col]}; "
            f"partitioning is by month, so it must be TIMESTAMP or DATE"
        )
    if col in table.get('update_columns', []):
        raise ValueError(
            f"{name}: partition_column {col} is also in update_columns. "
            f"A mutable partition key scatters the versions of one row across "
            f"partitions, where ReplacingMergeTree can never collapse them and "
            f"{name}_current returns duplicates. Partition on a column the "
            f"source never updates (created_at), not one it does."
        )


def topics(manifest):
    """Kafka topic per table, e.g. cdc.public.orders."""
    prefix = manifest.get('topic_prefix', 'cdc')
    schema = manifest.get('source_schema', 'public')
    return [f"{prefix}.{schema}.{t}" for t in manifest['tables']]


def table_include_list(manifest):
    """Debezium table.include.list value."""
    schema = manifest.get('source_schema', 'public')
    return ','.join(f"{schema}.{t}" for t in manifest['tables'])


def raw_ddl(name, table):
    """CREATE TABLE DDL for the raw.<name>_source warehouse table."""
    pk = table['primary_key']
    uniques = set(table.get('unique_columns', []))
    width = max(len(c) for c in table['columns'])
    lines = []
    for col, pg_type in table['columns'].items():
        suffix = ' PRIMARY KEY' if col == pk else (' UNIQUE' if col in uniques else '')
        lines.append(f"    {col.ljust(width)} {pg_type}{suffix}")
    cols = ',\n'.join(lines)
    return f"CREATE TABLE IF NOT EXISTS raw.{name}_source (\n{cols}\n)"


def _numeric_scale(pg_type):
    m = re.match(r'NUMERIC\((\d+)\s*,\s*(\d+)\)', pg_type.upper())
    return int(m.group(2)) if m else 2


def clickhouse_type(name, table, col):
    pg_type = table['columns'][col]
    base = re.sub(r'\(.*\)', '', pg_type).strip().upper()
    if base == 'NUMERIC':
        # Exact decimal, not Float64 — money must aggregate without float error
        ch = f'Decimal64({_numeric_scale(pg_type)})'
    else:
        ch = _PG_TO_CLICKHOUSE.get(base)
    if ch is None:
        raise ValueError(f"{name}.{col}: unsupported type {pg_type}")
    if ch == 'String' and col in table.get('low_cardinality', []):
        ch = 'LowCardinality(String)'
    return ch


def clickhouse_extract(table, col):
    """ClickHouse expression extracting `col` from the Debezium record JSON."""
    pg_type = table['columns'][col]
    base = re.sub(r'\(.*\)', '', pg_type).strip().upper()
    if base == 'TIMESTAMP':
        return f"toDateTime64(JSONExtractInt(rec, '{col}') / 1000000, 6)"
    if base == 'DATE':
        # Debezium encodes DATE as days since epoch
        return f"toDate(JSONExtractInt(rec, '{col}'))"
    if base == 'NUMERIC':
        # Parse the raw JSON number token as an exact decimal string.
        # Going via Float64 would truncate (19.99 -> 19.98) since the double
        # is 19.9899…; OrZero also makes nulls safe.
        return f"toDecimal64OrZero(JSONExtractRaw(rec, '{col}'), {_numeric_scale(pg_type)})"
    if base == 'SMALLINT':
        return f"toInt16(JSONExtractInt(rec, '{col}'))"
    if base == 'INTEGER':
        return f"toInt32(JSONExtractInt(rec, '{col}'))"
    if base == 'BIGINT':
        return f"JSONExtractInt(rec, '{col}')"
    if base == 'BOOLEAN':
        return f"JSONExtractBool(rec, '{col}')"
    if base == 'REAL':
        return f"toFloat32(JSONExtractFloat(rec, '{col}'))"
    if base == 'DOUBLE PRECISION':
        return f"JSONExtractFloat(rec, '{col}')"
    return f"JSONExtractString(rec, '{col}')"
