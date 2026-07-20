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
# decimal.handling.mode=double, timestamps as epoch micros)
_PG_TO_CLICKHOUSE = {
    'BIGINT': 'Int64',
    'INTEGER': 'Int32',
    'TEXT': 'String',
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
    return manifest


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
    if base == 'NUMERIC':
        return f"toDecimal64(JSONExtractFloat(rec, '{col}'), {_numeric_scale(pg_type)})"
    if base == 'INTEGER':
        return f"toInt32(JSONExtractInt(rec, '{col}'))"
    if base == 'BIGINT':
        return f"JSONExtractInt(rec, '{col}')"
    return f"JSONExtractString(rec, '{col}')"
