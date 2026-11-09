/**
 * The metrics endpoint is exempt from the dashboard's basic auth so that
 * Prometheus can reach it, so what goes into a label matters more here than
 * in most places. These cover the counting, the exposition format, and the
 * distinction that would otherwise make a governed platform look broken: a
 * permission error is the access policy working, not a failed query.
 *
 * Runs under `node --test` with no database and no API key.
 */
import { test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  recordAnswer,
  recordQuery,
  recordRefusal,
  recordSlaBlock,
  classifyError,
  snapshot,
  render,
  reset,
} from './metrics.ts';

beforeEach(() => reset());

function find(name: string) {
  const sample = snapshot().find((s) => s.name === name);
  // Throwing here rather than returning undefined keeps every assertion
  // below about the metric and not about the lookup.
  if (!sample) throw new Error(`no metric named ${name}`);
  return sample;
}

function value(name: string, label: string, want: string) {
  const found = find(name).values.find((v) => v.labels[label] === want);
  if (!found) throw new Error(`${name} has no sample with ${label}=${want}`);
  return found.value;
}

test('answers are counted against the store that produced them', () => {
  recordAnswer(['mirror']);
  recordAnswer(['mirror']);
  recordAnswer(['warehouse']);
  const values = find('etl_answers_total').values;
  assert.equal(value('etl_answers_total', 'store', 'mirror'), 2);
  assert.equal(value('etl_answers_total', 'store', 'warehouse'), 1);
});

test('an answer from two stores counts for both', () => {
  recordAnswer(['mirror', 'warehouse']);
  const values = find('etl_answers_total').values;
  assert.equal(values.length, 2);
  assert.ok(values.every((v) => v.value === 1));
});

test('an answer that queried nothing is still counted', () => {
  recordAnswer([]);
  assert.equal(find('etl_answers_total').values[0].labels.store, 'none');
});

test('a denied query is not counted as an error', () => {
  // The distinction that matters: counting the access policy as a failure
  // makes a correctly governed platform look broken.
  recordQuery('mirror', 'denied');
  recordQuery('mirror', 'error');
  assert.equal(value('etl_queries_total', 'outcome', 'denied'), 1);
  assert.equal(value('etl_queries_total', 'outcome', 'error'), 1);
});

test('a permission error is classified as denied, whichever store said it', () => {
  assert.equal(classifyError('permission denied for table customers_source'), 'denied');
  assert.equal(classifyError('DB::Exception: Not enough privileges'), 'denied');
  assert.equal(classifyError('ACCESS_DENIED'), 'denied');
});

test('an ordinary failure stays an error', () => {
  assert.equal(classifyError('syntax error at or near "slect"'), 'error');
  assert.equal(classifyError('relation "nope" does not exist'), 'error');
});

test('SLA blocks are counted per store', () => {
  recordSlaBlock('mirror');
  recordSlaBlock('mirror');
  recordSlaBlock('lakehouse');
  assert.equal(value('etl_store_sla_blocks_total', 'store', 'mirror'), 2);
});

test('refusals are counted even though nothing was queried', () => {
  recordRefusal();
  recordRefusal();
  assert.equal(find('etl_answer_refusals_total').values[0].value, 2);
});

test('the exposition format carries HELP and TYPE for each metric', () => {
  recordAnswer(['mirror']);
  const text = render(snapshot());
  assert.match(text, /# HELP etl_answers_total /);
  assert.match(text, /# TYPE etl_answers_total counter/);
  assert.match(text, /etl_answers_total\{store="mirror"\} 1/);
});

test('a metric with no observations is omitted rather than exposed as empty', () => {
  const text = render(snapshot());
  assert.doesNotMatch(text, /etl_answers_total\{/);
});

test('a metric with no labels renders without braces', () => {
  recordRefusal();
  assert.match(render(snapshot()), /etl_answer_refusals_total 1/);
});

test('label values are escaped so a quote cannot break the format', () => {
  recordQuery('we"ird', 'ok');
  const text = render(snapshot());
  assert.match(text, /store="we\\"ird"/);
  // Every sample line must still have balanced braces.
  for (const line of text.split('\n')) {
    if (!line || line.startsWith('#')) continue;
    const open = (line.match(/\{/g) || []).length;
    const close = (line.match(/\}/g) || []).length;
    assert.equal(open, close, line);
  }
});

test('a newline in a label cannot inject a second sample line', () => {
  recordQuery('a\nb', 'ok');
  const body = render(snapshot())
    .split('\n')
    .filter((l) => l.startsWith('etl_queries_total'));
  assert.equal(body.length, 1, 'a label broke the output into two samples');
});

test('reset clears everything, so one test cannot see another count', () => {
  recordAnswer(['mirror']);
  reset();
  assert.deepEqual(find('etl_answers_total').values, []);
});
