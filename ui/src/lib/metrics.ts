/**
 * What the governance layer did, as numbers Prometheus can scrape.
 *
 * Phase 5 of #231. The previous four phases each produce a signal that is
 * only visible one answer at a time: which store was chosen, whether it was
 * inside its SLA, how well tested it was, whether a query was refused by the
 * access policy. Those are the things you want a line on over a week, not a
 * log line you happen to read.
 *
 * This module holds no data, only counts. The endpoint it feeds is exempt
 * from the dashboard's basic auth so Prometheus can reach it, which makes the
 * rule here absolute rather than stylistic: **no SQL, no column values, no
 * question text.** Store names, table names and counters only. A metric label
 * is not a place to put something you would not publish, and
 * scripts/ci/test_metrics_safety.py fails the build over it.
 *
 * Counters live in the process and reset when it restarts. Prometheus handles
 * counter resets, and a second dashboard replica would simply report its own
 * -- fine for a signal, and not a basis for billing.
 */

export type QueryOutcome = 'ok' | 'error' | 'denied';

interface Counters {
  answers: Record<string, number>;
  queries: Record<string, number>;
  refusals: number;
  slaBlocked: Record<string, number>;
}

const counters: Counters = {
  answers: {},
  queries: {},
  refusals: 0,
  slaBlocked: {},
};

function bump(bucket: Record<string, number>, key: string, by = 1): void {
  bucket[key] = (bucket[key] ?? 0) + by;
}

/** One answer was produced, from these stores. */
export function recordAnswer(stores: string[]): void {
  if (!stores.length) {
    bump(counters.answers, 'none');
    return;
  }
  for (const store of stores) bump(counters.answers, store);
}

/**
 * One query ran.
 *
 * `denied` is separated from `error` on purpose: a permission error is the
 * access policy working, and counting it as a failure would make a correctly
 * governed platform look broken.
 */
export function recordQuery(store: string, outcome: QueryOutcome): void {
  bump(counters.queries, `${store}|${outcome}`);
}

/** The model refused, or the platform had no usable store to offer it. */
export function recordRefusal(): void {
  counters.refusals += 1;
}

/** A store was not offered because it was outside its freshness SLA. */
export function recordSlaBlock(store: string): void {
  bump(counters.slaBlocked, store);
}

/** Whether a database error was the access policy rather than a bad query. */
export function classifyError(message: string): QueryOutcome {
  return /permission denied|not enough privileges|access_denied/i.test(message)
    ? 'denied'
    : 'error';
}

export interface Sample {
  name: string;
  help: string;
  type: 'counter' | 'gauge';
  values: { labels: Record<string, string>; value: number }[];
}

/** Current counter state, as samples. */
export function snapshot(): Sample[] {
  const queries = Object.entries(counters.queries).map(([key, value]) => {
    const [store, outcome] = key.split('|');
    return { labels: { store, outcome }, value };
  });

  return [
    {
      name: 'etl_answers_total',
      help: 'Answers produced, by the store they were answered from.',
      type: 'counter',
      values: Object.entries(counters.answers).map(([store, value]) => ({
        labels: { store }, value,
      })),
    },
    {
      name: 'etl_queries_total',
      help: 'Queries run by the assistant. outcome=denied is the access policy refusing a column, not a failure.',
      type: 'counter',
      values: queries,
    },
    {
      name: 'etl_answer_refusals_total',
      help: 'Answers the platform declined to give, including when no store was within its SLA.',
      type: 'counter',
      values: [{ labels: {}, value: counters.refusals }],
    },
    {
      name: 'etl_store_sla_blocks_total',
      help: 'Times a store was withheld from the model because it was outside its freshness SLA.',
      type: 'counter',
      values: Object.entries(counters.slaBlocked).map(([store, value]) => ({
        labels: { store }, value,
      })),
    },
  ];
}

function escapeLabel(value: string): string {
  return value.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, ' ');
}

/** Prometheus text exposition format. */
export function render(samples: Sample[]): string {
  const lines: string[] = [];
  for (const sample of samples) {
    if (!sample.values.length) continue;
    lines.push(`# HELP ${sample.name} ${sample.help}`);
    lines.push(`# TYPE ${sample.name} ${sample.type}`);
    for (const { labels, value } of sample.values) {
      const pairs = Object.entries(labels)
        .filter(([, v]) => v !== undefined && v !== null)
        .map(([k, v]) => `${k}="${escapeLabel(String(v))}"`)
        .join(',');
      lines.push(pairs ? `${sample.name}{${pairs}} ${value}` : `${sample.name} ${value}`);
    }
  }
  return lines.join('\n') + '\n';
}

/** Test seam: counters are process state, so a test has to be able to clear them. */
export function reset(): void {
  counters.answers = {};
  counters.queries = {};
  counters.refusals = 0;
  counters.slaBlocked = {};
}
