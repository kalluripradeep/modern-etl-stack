/**
 * What the answer was actually built from (#231 phase 3).
 *
 * The model is already asked to say which store it used. That is a claim, and
 * a claim from the thing being audited is not evidence -- it is generated text
 * that happens to be about the query. The tool calls are the fact, so this
 * records those: the SQL that ran, where it ran, what came back, and the
 * freshness the routing decision rested on.
 *
 * Nothing here writes to a database, deliberately. The warehouse connection
 * the assistant holds is read-only by design, and an audit trail the audited
 * component can rewrite is not an audit trail. The record goes to stdout as
 * one structured line, which is where a cluster's log pipeline already
 * collects things that must outlive the process.
 *
 * Like routing.ts, this module imports nothing: the assembly has to be
 * reproducible and is tested under `node --test` with no database and no key.
 */

export interface QueryRecord {
  /** The store the query ran against, from the tool that was called. */
  store: string;
  sql: string;
  rows: number | null;
  ms: number;
  error?: string;
}

export interface ColumnExposure {
  table: string;
  classification: string;
  columns: string[];
}

export interface ProvenanceRecord {
  traceId: string;
  at: string;
  question: string;
  storesQueried: string[];
  queries: QueryRecord[];
  storeHealth: { store: string; freshnessSeconds: number | null; eligible: boolean; reason: string }[];
  exposed: ColumnExposure[];
  answered: boolean;
}

/** Short, sortable, and unique enough to tie a log line to a reply. */
export function newTraceId(now: number = Date.now(), rand: number = Math.random()): string {
  const stamp = now.toString(36);
  const salt = Math.floor(rand * 36 ** 6).toString(36).padStart(6, '0');
  return `${stamp}-${salt}`;
}

/**
 * Which known tables a statement referenced.
 *
 * Substring matching against the physical names the manifest already knows,
 * not SQL parsing. That is an approximation and is labelled as one wherever
 * it surfaces: it can over-report a name that appears inside a string
 * literal, and it will miss a table reached through a view it does not know
 * about. It is useful for "did this answer touch personal data", which is the
 * question being asked, and it is not a substitute for column-level lineage.
 *
 * Over-reporting is the safe direction for that question, which is why the
 * approximation is acceptable here and would not be if it drove masking.
 */
export function tablesReferenced(sql: string, knownTables: string[]): string[] {
  const haystack = sql.toLowerCase();
  return knownTables
    .filter((name) => haystack.includes(name.toLowerCase()))
    .sort();
}

/**
 * The classified columns a set of queries could have exposed.
 *
 * `contract` is phase 0's map of table -> column -> classification, keyed by
 * the same physical names used in the SQL. Only classifications the caller
 * asks about are reported, so the common case -- "did anything personal
 * appear" -- does not drown in a list of every internal column.
 */
export function exposedColumns(
  queries: QueryRecord[],
  contract: Record<string, Record<string, string>>,
  report: string[],
): ColumnExposure[] {
  const known = Object.keys(contract);
  const found = new Map<string, ColumnExposure>();

  for (const query of queries) {
    for (const table of tablesReferenced(query.sql, known)) {
      for (const [column, classification] of Object.entries(contract[table])) {
        if (!report.includes(classification)) continue;
        const key = `${table}:${classification}`;
        const entry = found.get(key) ?? { table, classification, columns: [] };
        if (!entry.columns.includes(column)) entry.columns.push(column);
        found.set(key, entry);
      }
    }
  }

  return [...found.values()]
    .map((e) => ({ ...e, columns: [...e.columns].sort() }))
    .sort((a, b) => a.table.localeCompare(b.table) || a.classification.localeCompare(b.classification));
}

/** One line a reader can act on, for the end of the answer. */
export function summarise(record: ProvenanceRecord): string {
  if (!record.queries.length) {
    return `No store was queried. trace ${record.traceId}`;
  }

  const parts = record.storesQueried.map((store) => {
    const health = record.storeHealth.find((h) => h.store === store);
    const age =
      health?.freshnessSeconds === null || health?.freshnessSeconds === undefined
        ? 'freshness unknown'
        : `${Math.round(health.freshnessSeconds)}s behind`;
    return `${store} (${age})`;
  });

  const sensitive = record.exposed.filter((e) => e.classification !== 'internal');
  const warning = sensitive.length
    ? ` Touched ${sensitive.map((e) => `${e.classification} columns in ${e.table}`).join(', ')}.`
    : '';

  const count = record.queries.length === 1 ? '1 query' : `${record.queries.length} queries`;
  return `Answered from ${parts.join(' and ')} — ${count}.${warning} trace ${record.traceId}`;
}
