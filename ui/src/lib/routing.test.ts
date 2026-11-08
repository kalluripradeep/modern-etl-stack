/**
 * The routing decision has to be reproducible, so it is tested without a
 * database, a cluster or an API key. Run with `npm test` (node --test).
 *
 * The cases are the ones that were wrong in production rather than the happy
 * path: a store past its SLA (#188, the mirror at 84s against a 30s target),
 * a store that cannot be probed at all (#225, a day behind a connector task
 * still reporting RUNNING), and an untested table, which phase 1 scores NULL
 * precisely so that nobody can read it as healthy.
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  assess,
  describe as describeVerdicts,
  type StoreReading,
  type RoutingPolicy,
} from './routing.ts';

const POLICY: RoutingPolicy = {
  staleExcludesStore: true,
  qualityWarnBelow: 0.95,
  measurementTtlSeconds: 10,
};

const reading = (over: Partial<StoreReading> = {}): StoreReading => ({
  store: 'mirror',
  slaSeconds: 30,
  freshnessSeconds: 3,
  quality: 1,
  ...over,
});

test('a fresh, tested store is usable', () => {
  const [v] = assess([reading()], POLICY);
  assert.equal(v.eligible, true);
  assert.equal(v.withinSla, true);
  assert.equal(v.qualityWarning, false);
});

test('a store exactly at its SLA is still within it', () => {
  const [v] = assess([reading({ freshnessSeconds: 30 })], POLICY);
  assert.equal(v.withinSla, true);
  assert.equal(v.eligible, true);
});

test('one second past the SLA is not', () => {
  const [v] = assess([reading({ freshnessSeconds: 31 })], POLICY);
  assert.equal(v.withinSla, false);
  assert.equal(v.eligible, false);
});

test('the 84 second mirror from #188 is excluded, with the number in the reason', () => {
  const [v] = assess([reading({ freshnessSeconds: 84 })], POLICY);
  assert.equal(v.eligible, false);
  assert.match(v.reason, /84s behind/);
  assert.match(v.reason, /SLA 30s/);
});

test('a probe that could not run excludes the store rather than assuming it is fine', () => {
  const [v] = assess([reading({ freshnessSeconds: null })], POLICY);
  assert.equal(v.eligible, false);
  assert.equal(v.withinSla, false);
  assert.match(v.reason, /could not be measured/);
});

test('an untested store is flagged unknown but stays usable', () => {
  const [v] = assess([reading({ quality: null })], POLICY);
  assert.equal(v.qualityWarning, true, 'NULL quality must never read as passing');
  assert.equal(v.eligible, true, 'untested is not by itself a reason to refuse the data');
});

test('a quality score below the threshold warns without excluding', () => {
  const [v] = assess([reading({ quality: 0.8 })], POLICY);
  assert.equal(v.qualityWarning, true);
  assert.equal(v.eligible, true);
});

test('a policy that does not exclude downgrades staleness to a warning', () => {
  const lenient: RoutingPolicy = { ...POLICY, staleExcludesStore: false };
  const [v] = assess([reading({ freshnessSeconds: 999 })], lenient);
  assert.equal(v.withinSla, false);
  assert.equal(v.eligible, true);
  assert.match(v.reason, /policy allows/);
});

test('stores are judged independently', () => {
  const verdicts = assess(
    [
      reading({ store: 'mirror', freshnessSeconds: 5 }),
      reading({
        store: 'warehouse',
        slaSeconds: 7200,
        freshnessSeconds: 1924838,
        quality: null,
      }),
    ],
    POLICY,
  );
  assert.equal(verdicts[0].eligible, true);
  assert.equal(verdicts[1].eligible, false, 'a 22 day old warehouse is not usable');
});

test('when nothing is within SLA the model is told to refuse, not to pick the best of a bad set', () => {
  const text = describeVerdicts(
    assess(
      [
        reading({ freshnessSeconds: 9000 }),
        reading({ store: 'warehouse', slaSeconds: 7200, freshnessSeconds: 999999 }),
      ],
      POLICY,
    ),
  );
  assert.match(text, /No store is within its freshness SLA/);
  assert.doesNotMatch(text, /Answer from one of/);
});

test('the description carries the measurement, so the answer can cite it', () => {
  const text = describeVerdicts(assess([reading({ freshnessSeconds: 7 })], POLICY));
  assert.match(text, /Measured at request time/);
  assert.match(text, /7s behind/);
  assert.match(text, /State which store you used/);
});
