/**
 * The metadata dbt already produces, served read-only (#231 phase 1).
 *
 * Phase 1 asked for test results and the lineage graph behind one read-only
 * endpoint. The quality half reached `/api/metrics` as Prometheus gauges,
 * which suits alerting and suits nothing else: a gauge cannot carry an edge,
 * so the lineage graph stayed in `meta.dbt_lineage` readable only by whoever
 * had warehouse credentials. This is the other half.
 *
 * Unlike `/api/metrics` this route is **not** exempt from the dashboard's
 * basic auth. The exemption exists because Prometheus cannot hold a password,
 * and it is defensible only because that route emits counts. This one emits
 * table names, column-free but structural, and nothing needs it unauthed.
 *
 * Read-only by construction: two fixed SELECTs, no parameters, no user input
 * reaching SQL.
 */
import { NextResponse } from 'next/server';
import { queryWarehouse } from '@/lib/datastores';

export const dynamic = 'force-dynamic';

interface QualityRow {
  table: string;
  tests_total: number;
  tests_passed: number;
  failures: number;
  /** null means untested -- nobody looked. Not 0, and emphatically not 1. */
  score: number | null;
  run_at: string | null;
}

async function quality(): Promise<{ rows: QualityRow[]; truncated: boolean } | null> {
  try {
    const result = await queryWarehouse(
      'SELECT table_name, tests_total, tests_passed, failures, score, run_at '
      + 'FROM meta.dbt_quality ORDER BY table_name',
    );
    return {
      rows: result.rows.map(([table, total, passed, failures, score, runAt]) => ({
        table: String(table),
        tests_total: Number(total),
        tests_passed: Number(passed),
        failures: Number(failures),
        // Postgres NUMERIC arrives as a string; null has to survive as null.
        score: score === null || score === undefined ? null : Number(score),
        // node-postgres hands back a Date, whose toString() is
        // "Sat Nov 14 2026 11:54:33 GMT+0000 (Greenwich Mean Time)" -- a
        // locale-shaped string no caller should have to parse. ISO 8601.
        run_at: runAt instanceof Date ? runAt.toISOString()
          : runAt === null || runAt === undefined ? null : String(runAt),
      })),
      truncated: result.truncated,
    };
  } catch {
    return null;
  }
}

async function lineage(): Promise<{ edges: { child: string; parent: string }[]; truncated: boolean } | null> {
  try {
    const result = await queryWarehouse(
      'SELECT child, parent FROM meta.dbt_lineage ORDER BY child, parent',
    );
    return {
      edges: result.rows.map(([child, parent]) => ({
        child: String(child), parent: String(parent),
      })),
      truncated: result.truncated,
    };
  } catch {
    return null;
  }
}

export async function GET() {
  const [q, l] = await Promise.all([quality(), lineage()]);

  // meta.* does not exist until the ingest DAG has run once, and the role may
  // not be able to read it. Either way this says which half is missing rather
  // than returning a 500 that looks like the endpoint is broken.
  if (!q && !l) {
    return NextResponse.json(
      {
        error: 'no dbt metadata available',
        detail: 'meta.dbt_quality and meta.dbt_lineage are written at the end '
          + 'of each ingest run; neither could be read.',
      },
      { status: 503 },
    );
  }

  return NextResponse.json({
    quality: q?.rows ?? null,
    lineage: l?.edges ?? null,
    // A caller plotting a graph needs to know it is looking at part of one.
    truncated: Boolean(q?.truncated || l?.truncated),
    note: 'score is null for a table with no tests -- untested, not passing.',
  });
}
