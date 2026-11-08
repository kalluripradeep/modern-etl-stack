/**
 * Where should this question be answered from? (#231 phase 2)
 *
 * The assistant used to decide from two adjectives in its system prompt:
 * "seconds fresh" for the mirror, "batch, daily fresh" for the warehouse.
 * Issue #188 measured the mirror at 84 seconds and #225 found it a day behind
 * a connector task that was still reporting RUNNING. On both occasions an
 * agent reading those adjectives would have answered a question about *now*
 * from data that was not, confidently, with nothing to show the reader.
 *
 * So the adjectives are replaced by a measurement taken at request time. The
 * `freshness` expression for each store has been in the manifest since the
 * stores were declared and had never been run by anything; this is its first
 * consumer.
 *
 * `assess` is deliberately pure. The decision is the part that has to be
 * reproducible and auditable -- the same inputs must always produce the same
 * verdict and the same stated reason -- so it takes measurements rather than
 * connections, and is tested without a database or an API key.
 *
 * This module imports nothing on purpose. The policy and the store labels are
 * passed in by the caller, which is what lets `node --test` run it directly
 * against the TypeScript with no bundler, no mocking and no test framework.
 */

/** Shape of the `routing_policy` block generated from the manifest. */
export interface RoutingPolicy {
  staleExcludesStore: boolean;
  qualityWarnBelow: number;
  measurementTtlSeconds: number;
}

export interface StoreReading {
  store: string;
  slaSeconds: number;
  /** Seconds behind, or null when the probe could not be run. */
  freshnessSeconds: number | null;
  /** dbt test score, or null when the store has no tests -- unknown, not good. */
  quality: number | null;
}

export interface StoreVerdict extends StoreReading {
  withinSla: boolean;
  eligible: boolean;
  qualityWarning: boolean;
  reason: string;
}

function describeAge(seconds: number): string {
  if (seconds < 90) return Math.round(seconds) + 's';
  if (seconds < 5400) return Math.round(seconds / 60) + 'm';
  if (seconds < 172800) return Math.round(seconds / 3600) + 'h';
  return Math.round(seconds / 86400) + 'd';
}

export function assess(
  readings: StoreReading[],
  policy: RoutingPolicy,
): StoreVerdict[] {
  return readings.map((reading) => {
    const { freshnessSeconds, slaSeconds, quality } = reading;

    // A store that cannot say how old it is cannot be trusted to be current.
    // Treating an unanswerable probe as "probably fine" is the failure this
    // whole phase exists to stop.
    if (freshnessSeconds === null) {
      return {
        ...reading,
        withinSla: false,
        eligible: false,
        qualityWarning: quality === null || quality < policy.qualityWarnBelow,
        reason: 'freshness could not be measured',
      };
    }

    const withinSla = freshnessSeconds <= slaSeconds;
    // null quality means untested -- phase 1 leaves it NULL rather than 1.0.
    // Unknown is reported as unknown and never counts as having passed.
    const qualityWarning = quality === null || quality < policy.qualityWarnBelow;

    if (!withinSla && policy.staleExcludesStore) {
      return {
        ...reading,
        withinSla,
        eligible: false,
        qualityWarning,
        reason:
          describeAge(freshnessSeconds) + ' behind, SLA ' + describeAge(slaSeconds),
      };
    }

    const reason = withinSla
      ? describeAge(freshnessSeconds) + ' behind, within SLA ' + describeAge(slaSeconds)
      : describeAge(freshnessSeconds)
        + ' behind, SLA ' + describeAge(slaSeconds)
        + ' -- stale, policy allows';

    return { ...reading, withinSla, eligible: true, qualityWarning, reason };
  });
}

/** The facts the model is given in place of the adjectives it used to get. */
export function describe(
  verdicts: StoreVerdict[],
  labels: Record<string, string> = {},
): string {
  const lines = verdicts.map((v) => {
    const label = labels[v.store] ?? v.store;
    const quality =
      v.quality === null ? 'quality unknown (no dbt tests)' : 'quality ' + v.quality;
    const status = v.eligible ? 'USABLE' : 'DO NOT USE';
    return '- ' + v.store + ' (' + label + '): ' + status + ' -- ' + v.reason + '; ' + quality;
  });

  const usable = verdicts.filter((v) => v.eligible);
  const footer = usable.length
    ? 'Answer from one of: '
      + usable.map((v) => v.store).join(', ')
      + '. State which store you used and how far behind it was.'
    : 'No store is within its freshness SLA right now. Say so plainly, give the '
      + 'measured lag, and do not present any figure as current.';

  return 'Measured at request time, not assumed:\n' + lines.join('\n') + '\n\n' + footer;
}
