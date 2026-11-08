/**
 * Measure what the routing policy judges (#231 phase 2).
 *
 * Kept apart from routing.ts, which decides. This half talks to three
 * databases and therefore cannot be unit tested without them; the half that
 * decides imports nothing and is tested on its own. Splitting them is what
 * stops the interesting logic from being the untestable logic.
 */
import {
  STORES,
  STORE_NAMES,
  ROUTING_POLICY,
} from './stores.generated.ts';
import { queryWarehouse, queryLakehouse, queryMirror } from './datastores.ts';
import { assess, describe, type StoreReading, type StoreVerdict } from './routing.ts';

type Runner = (sql: string) => Promise<{ rows: unknown[][] }>;

const RUNNERS: Record<string, Runner> = {
  warehouse: queryWarehouse,
  lakehouse: queryLakehouse,
  mirror: queryMirror,
};

function firstNumber(rows: unknown[][]): number | null {
  const value = rows?.[0]?.[0];
  if (value === null || value === undefined) return null;
  const n = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(n) ? n : null;
}

/**
 * dbt test scores, keyed by physical table name (phase 1).
 *
 * A missing meta schema is not an error: it means the ingest DAG has not run
 * since phase 1 landed. Every store is then "unknown", which the policy
 * already treats as not-passing rather than as healthy.
 */
async function qualityByTable(): Promise<Record<string, number | null>> {
  try {
    const result = await queryWarehouse(
      'SELECT table_name, score FROM meta.dbt_quality',
    );
    const scores: Record<string, number | null> = {};
    for (const [name, score] of result.rows) {
      scores[String(name)] = score === null ? null : Number(score);
    }
    return scores;
  } catch {
    return {};
  }
}

let cached: { at: number; verdicts: StoreVerdict[] } | null = null;

/**
 * One reading per store, taken now.
 *
 * Probes run in parallel and a failure is a null rather than a throw: a store
 * that cannot answer is a routing input, not an outage. The cache exists
 * because a reading older than the tightest SLA cannot judge it -- the mirror
 * allows 30s, so `measurementTtlSeconds` is well inside that.
 */
export async function measureStores(): Promise<StoreVerdict[]> {
  const now = Date.now();
  if (cached && now - cached.at < ROUTING_POLICY.measurementTtlSeconds * 1000) {
    return cached.verdicts;
  }

  try {
    return await takeReadings(now);
  } catch {
    // Health checking must never be the thing that breaks the endpoint. An
    // unexpected failure here degrades to "nothing could be measured", which
    // the policy already treats as unusable -- the conservative answer, and
    // the same one a dead store gets.
    return assess(
      STORE_NAMES.map((store) => ({
        store,
        slaSeconds: STORES[store].slaFreshnessSeconds,
        freshnessSeconds: null,
        quality: null,
      })),
      ROUTING_POLICY,
    );
  }
}

async function takeReadings(now: number): Promise<StoreVerdict[]> {
  const quality = await qualityByTable();

  const readings: StoreReading[] = await Promise.all(
    STORE_NAMES.map(async (store) => {
      const def = STORES[store];
      const run = RUNNERS[store];
      let freshnessSeconds: number | null = null;
      if (run) {
        try {
          freshnessSeconds = firstNumber((await run(def.freshnessSql)).rows);
        } catch {
          freshnessSeconds = null;
        }
      }
      // The probe table under this store's own naming, which is the key the
      // quality scores are written under.
      const probeTable = def.tables.orders;
      return {
        store,
        slaSeconds: def.slaFreshnessSeconds,
        freshnessSeconds,
        quality: probeTable in quality ? quality[probeTable] : null,
      };
    }),
  );

  const verdicts = assess(readings, ROUTING_POLICY);
  cached = { at: now, verdicts };
  return verdicts;
}

export function describeStores(verdicts: StoreVerdict[]): string {
  const labels: Record<string, string> = {};
  for (const name of STORE_NAMES) labels[name] = STORES[name].label;
  return describe(verdicts, labels);
}

/** For the provenance line on the answer (#231 phase 3 reads this too). */
export function provenance(verdicts: StoreVerdict[]): string {
  return verdicts
    .map((v) => {
      const age = v.freshnessSeconds === null ? 'unmeasurable' : `${Math.round(v.freshnessSeconds)}s`;
      return `${v.store}=${age}${v.eligible ? '' : ' (excluded)'}`;
    })
    .join(' · ');
}
