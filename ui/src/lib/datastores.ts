import { Client } from 'pg';

/**
 * Read-only clients for the three data stores the assistant can query:
 *  - warehouse:  PostgreSQL (live CDC mirror in public.*, dbt marts in int/prs)
 *  - lakehouse:  Trino over Iceberg (silver layer, schema `lake`)
 *  - mirror:     ClickHouse columnar CDC mirror (database `mirror`)
 */

export interface SqlResult {
  columns: string[];
  rows: unknown[][];
  rowCount: number;
  truncated: boolean;
}

export const MAX_RESULT_ROWS = 100;
const QUERY_TIMEOUT_MS = 20000;

/**
 * LLM output is untrusted input: only a single SELECT/WITH statement passes.
 * Each store additionally enforces read-only server-side.
 */
export function assertReadOnly(sql: string): string {
  const stripped = sql
    .replace(/--.*$/gm, '')
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .trim()
    .replace(/;\s*$/, '');
  if (stripped.length === 0 || stripped.includes(';')) {
    throw new Error('Only a single SQL statement is allowed.');
  }
  if (!/^(select|with)\b/i.test(stripped)) {
    throw new Error('Only read-only SELECT (or WITH ... SELECT) queries are allowed.');
  }
  return stripped;
}

function truncate(columns: string[], rows: unknown[][]): SqlResult {
  return {
    columns,
    rows: rows.slice(0, MAX_RESULT_ROWS),
    rowCount: rows.length,
    truncated: rows.length > MAX_RESULT_ROWS,
  };
}

// ── PostgreSQL warehouse ────────────────────────────────────────────────────

function pgClient(): Client {
  return new Client({
    user: process.env.DEST_DB_USER || 'destuser',
    host: process.env.DEST_DB_HOST || 'localhost',
    database: process.env.DEST_DB_NAME || 'destdb',
    password: process.env.DEST_DB_PASSWORD || 'destpass',
    port: parseInt(process.env.DEST_DB_PORT || '5433'),
    options: `-c default_transaction_read_only=on -c statement_timeout=${QUERY_TIMEOUT_MS}`,
  });
}

export async function queryWarehouse(sql: string): Promise<SqlResult> {
  const checked = assertReadOnly(sql);
  const client = pgClient();
  await client.connect();
  try {
    const res = await client.query({ text: checked, rowMode: 'array' });
    const columns = res.fields.map((f) => f.name);
    return truncate(columns, res.rows as unknown[][]);
  } finally {
    await client.end();
  }
}

// ── Trino lakehouse ─────────────────────────────────────────────────────────

export function trinoConfigured(): boolean {
  return Boolean(process.env.TRINO_URL);
}

export async function queryLakehouse(sql: string): Promise<SqlResult> {
  const base = process.env.TRINO_URL;
  if (!base) {
    throw new Error('Lakehouse is not configured (set TRINO_URL, e.g. http://trino:8080).');
  }
  const checked = assertReadOnly(sql);

  let res = await fetch(`${base}/v1/statement`, {
    method: 'POST',
    headers: { 'X-Trino-User': 'dashboard' },
    body: checked,
    signal: AbortSignal.timeout(QUERY_TIMEOUT_MS),
  });

  const columns: string[] = [];
  const rows: unknown[][] = [];
  const deadline = Date.now() + QUERY_TIMEOUT_MS;

  // Trino's REST protocol pages results: follow nextUri until absent
  for (;;) {
    if (!res.ok) throw new Error(`Trino HTTP ${res.status}: ${await res.text()}`);
    const page = await res.json();
    if (page.error) {
      throw new Error(`Trino: ${page.error.message}`);
    }
    if (page.columns && columns.length === 0) {
      for (const c of page.columns) columns.push(c.name);
    }
    if (page.data) rows.push(...page.data);
    if (!page.nextUri) break;
    if (Date.now() > deadline) throw new Error('Trino query timed out.');
    res = await fetch(page.nextUri, {
      headers: { 'X-Trino-User': 'dashboard' },
      signal: AbortSignal.timeout(QUERY_TIMEOUT_MS),
    });
  }
  return truncate(columns, rows);
}

// ── ClickHouse columnar mirror ──────────────────────────────────────────────

export function clickhouseConfigured(): boolean {
  return Boolean(process.env.CLICKHOUSE_URL);
}

export async function queryMirror(sql: string): Promise<SqlResult> {
  const base = process.env.CLICKHOUSE_URL;
  if (!base) {
    throw new Error('Columnar mirror is not configured (set CLICKHOUSE_URL, e.g. http://clickhouse:8123).');
  }
  const checked = assertReadOnly(sql);

  // readonly=1 makes the whole session read-only server-side
  const res = await fetch(`${base}/?default_format=JSONCompact&readonly=1`, {
    method: 'POST',
    headers: {
      'X-ClickHouse-User': process.env.CLICKHOUSE_USER || 'chuser',
      'X-ClickHouse-Key': process.env.CLICKHOUSE_PASSWORD || 'chpass',
    },
    body: checked,
    signal: AbortSignal.timeout(QUERY_TIMEOUT_MS),
  });
  if (!res.ok) {
    throw new Error(`ClickHouse: ${(await res.text()).slice(0, 500)}`);
  }
  const body = await res.json();
  const columns = (body.meta || []).map((m: { name: string }) => m.name);
  return truncate(columns, body.data || []);
}

// ── Schema overview across all stores ───────────────────────────────────────

export async function getSchemaOverview(): Promise<string> {
  const parts: string[] = [];

  try {
    const pg = await queryWarehouse(`
      SELECT table_schema || '.' || table_name AS tbl,
             string_agg(column_name || ' ' || data_type, ', ' ORDER BY ordinal_position) AS cols
      FROM information_schema.columns
      WHERE table_schema IN ('public', 'raw', 'int', 'prs')
      GROUP BY 1 ORDER BY 1
    `);
    parts.push(
      'WAREHOUSE (PostgreSQL — query_warehouse; public.* = live CDC mirror, raw/int/prs = dbt layers):\n' +
      pg.rows.map(([t, c]) => `  ${t}: ${c}`).join('\n'),
    );
  } catch (e) {
    parts.push(`WAREHOUSE: unavailable (${e instanceof Error ? e.message : e})`);
  }

  if (trinoConfigured()) {
    try {
      const tr = await queryLakehouse(`
        SELECT table_schema || '.' || table_name,
               array_join(array_agg(column_name || ' ' || data_type), ', ')
        FROM iceberg.information_schema.columns
        WHERE table_schema = 'lake'
        GROUP BY 1 ORDER BY 1
      `);
      parts.push(
        'LAKEHOUSE (Trino over Iceberg — query_lakehouse; prefix tables with iceberg., e.g. iceberg.lake.orders):\n' +
        (tr.rows.length
          ? tr.rows.map(([t, c]) => `  iceberg.${t}: ${c}`).join('\n')
          : '  (no tables yet — the Spark silver pipeline has not run)'),
      );
    } catch (e) {
      parts.push(`LAKEHOUSE: unavailable (${e instanceof Error ? e.message : e})`);
    }
  }

  if (clickhouseConfigured()) {
    try {
      const ch = await queryMirror(`
        SELECT concat(database, '.', table), arrayStringConcat(groupArray(concat(name, ' ', type)), ', ')
        FROM system.columns
        WHERE database = 'mirror' AND table LIKE '%_current'
        GROUP BY 1 ORDER BY 1
      `);
      parts.push(
        'COLUMNAR MIRROR (ClickHouse — query_mirror; use the *_current views for correct results):\n' +
        ch.rows.map(([t, c]) => `  ${t}: ${c}`).join('\n'),
      );
    } catch (e) {
      parts.push(`COLUMNAR MIRROR: unavailable (${e instanceof Error ? e.message : e})`);
    }
  }

  return parts.join('\n\n');
}

// ── Markdown rendering ──────────────────────────────────────────────────────

export function resultToMarkdown(result: SqlResult): string {
  if (result.columns.length === 0 || result.rowCount === 0) {
    return '(no rows)';
  }
  const esc = (v: unknown) => String(v ?? '').replace(/\|/g, '\\|').replace(/\n/g, ' ');
  const lines = [
    `| ${result.columns.join(' | ')} |`,
    `| ${result.columns.map(() => '---').join(' | ')} |`,
    ...result.rows.map((r) => `| ${r.map(esc).join(' | ')} |`),
  ];
  if (result.truncated) {
    lines.push('', `*(showing first ${MAX_RESULT_ROWS} of ${result.rowCount} rows)*`);
  }
  return lines.join('\n');
}
