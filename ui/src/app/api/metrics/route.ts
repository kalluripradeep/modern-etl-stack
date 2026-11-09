/**
 * Prometheus scrape target for the governance layer (#231 phase 5).
 *
 * Exempt from the dashboard's basic auth in src/proxy.ts, because a scrape
 * job cannot hold a password without that password living in a ConfigMap.
 * That exemption is why this route is careful about what it emits: store
 * names, table names and numbers. **No SQL, no column values, no question
 * text** -- scripts/ci/test_metrics_safety.py fails the build over it.
 *
 * Counters come from the process; gauges are measured per scrape. Freshness
 * is already cached inside `measurementTtlSeconds`, so a scrape every 15s
 * does not add three queries every 15s.
 */
import { NextResponse } from 'next/server';
import { snapshot, render, type Sample } from '@/lib/metrics';
import { measureStores } from '@/lib/store-health';
import { queryWarehouse } from '@/lib/datastores';

export const dynamic = 'force-dynamic';

/**
 * Per-table dbt test outcomes (phase 1).
 *
 * A table with no tests is deliberately absent from the score gauge rather
 * than reported as 0 or 1: NULL means nobody looked, and a gauge cannot say
 * that. It is counted in etl_dbt_untested_tables instead, which is the
 * number worth alerting on.
 */
async function qualitySamples(): Promise<Sample[]> {
  try {
    const result = await queryWarehouse(
      'SELECT table_name, score, tests_total FROM meta.dbt_quality',
    );
    const scored: Sample['values'] = [];
    const tests: Sample['values'] = [];
    let untested = 0;

    for (const [name, score, total] of result.rows) {
      const table = String(name);
      tests.push({ labels: { table }, value: Number(total) });
      if (score === null || score === undefined) untested += 1;
      else scored.push({ labels: { table }, value: Number(score) });
    }

    return [
      {
        name: 'etl_dbt_quality_score',
        help: 'Fraction of dbt tests passing per table. Tables with no tests are absent, not 1.',
        type: 'gauge',
        values: scored,
      },
      {
        name: 'etl_dbt_tests_total',
        help: 'Number of dbt tests covering each table.',
        type: 'gauge',
        values: tests,
      },
      {
        name: 'etl_dbt_untested_tables',
        help: 'Tables with no dbt tests at all. Untested is not the same as passing.',
        type: 'gauge',
        values: [{ labels: {}, value: untested }],
      },
    ];
  } catch {
    // meta.dbt_quality does not exist until the ingest DAG has run once.
    // A scrape should degrade to fewer metrics, never to a 500.
    return [];
  }
}

async function storeSamples(): Promise<Sample[]> {
  try {
    const verdicts = await measureStores();
    return [
      {
        name: 'etl_store_freshness_seconds',
        help: 'Measured lag per store. Absent when the probe could not be run.',
        type: 'gauge',
        values: verdicts
          .filter((v) => v.freshnessSeconds !== null)
          .map((v) => ({ labels: { store: v.store }, value: v.freshnessSeconds as number })),
      },
      {
        name: 'etl_store_sla_seconds',
        help: 'Freshness target per store, from the manifest.',
        type: 'gauge',
        values: verdicts.map((v) => ({ labels: { store: v.store }, value: v.slaSeconds })),
      },
      {
        name: 'etl_store_eligible',
        help: '1 when a store is within its SLA and may be used, 0 otherwise.',
        type: 'gauge',
        values: verdicts.map((v) => ({ labels: { store: v.store }, value: v.eligible ? 1 : 0 })),
      },
      {
        name: 'etl_store_probe_failed',
        help: '1 when a store could not report its own freshness. Excluded, not assumed healthy.',
        type: 'gauge',
        values: verdicts.map((v) => ({
          labels: { store: v.store }, value: v.freshnessSeconds === null ? 1 : 0,
        })),
      },
    ];
  } catch {
    return [];
  }
}

export async function GET() {
  const [stores, quality] = await Promise.all([storeSamples(), qualitySamples()]);
  const body = render([...snapshot(), ...stores, ...quality]);
  return new NextResponse(body, {
    status: 200,
    headers: { 'content-type': 'text/plain; version=0.0.4; charset=utf-8' },
  });
}
