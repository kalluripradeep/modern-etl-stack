/**
 * Provenance has to be right about what happened, which means it is tested
 * against the ways it could be wrong rather than the happy path: a table name
 * that is a prefix of another, a query that failed, a question answered with
 * no query at all, and personal columns being reported when they are touched
 * and not when they are not.
 *
 * Runs under `node --test` with no database and no API key.
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  newTraceId,
  tablesReferenced,
  exposedColumns,
  summarise,
  type QueryRecord,
  type ProvenanceRecord,
} from './provenance.ts';

const CONTRACT: Record<string, Record<string, string>> = {
  'mirror.customers_current': {
    customer_id: 'internal',
    email: 'pii',
    first_name: 'pii',
    created_at: 'internal',
  },
  'mirror.orders_current': {
    order_id: 'internal',
    total_amount: 'internal',
  },
  'mirror.order_items_current': {
    item_id: 'internal',
  },
};

const query = (over: Partial<QueryRecord> = {}): QueryRecord => ({
  store: 'mirror',
  sql: 'SELECT count(*) FROM mirror.orders_current',
  rows: 1,
  ms: 12,
  ...over,
});

const record = (over: Partial<ProvenanceRecord> = {}): ProvenanceRecord => ({
  traceId: 'abc-123456',
  at: '2026-11-08T12:00:00.000Z',
  question: 'how many orders',
  storesQueried: ['mirror'],
  queries: [query()],
  storeHealth: [{ store: 'mirror', freshnessSeconds: 4, eligible: true, reason: 'within SLA' }],
  exposed: [],
  answered: true,
  ...over,
});

test('trace ids are unique and stable for given inputs', () => {
  assert.equal(newTraceId(1000, 0.5), newTraceId(1000, 0.5));
  assert.notEqual(newTraceId(1000, 0.5), newTraceId(1001, 0.5));
  assert.notEqual(newTraceId(1000, 0.5), newTraceId(1000, 0.9));
});

test('a referenced table is found regardless of case', () => {
  const found = tablesReferenced(
    'select * from MIRROR.ORDERS_CURRENT limit 1',
    Object.keys(CONTRACT),
  );
  assert.deepEqual(found, ['mirror.orders_current']);
});

test('a table whose name is a prefix of another does not drag it in', () => {
  // mirror.orders_current is NOT a substring of mirror.order_items_current,
  // but the reverse direction is the trap: a careless match on "orders"
  // would claim both.
  const found = tablesReferenced(
    'SELECT * FROM mirror.order_items_current',
    Object.keys(CONTRACT),
  );
  assert.deepEqual(found, ['mirror.order_items_current']);
});

test('a join reports every table it touched', () => {
  const found = tablesReferenced(
    'SELECT c.email FROM mirror.orders_current o JOIN mirror.customers_current c ON c.customer_id = o.order_id',
    Object.keys(CONTRACT),
  );
  assert.deepEqual(found, ['mirror.customers_current', 'mirror.orders_current']);
});

test('personal columns are reported when a query touches their table', () => {
  const exposed = exposedColumns(
    [query({ sql: 'SELECT email FROM mirror.customers_current' })],
    CONTRACT,
    ['pii', 'phi'],
  );
  assert.equal(exposed.length, 1);
  assert.equal(exposed[0].table, 'mirror.customers_current');
  assert.equal(exposed[0].classification, 'pii');
  assert.deepEqual(exposed[0].columns, ['email', 'first_name']);
});

test('a query touching nothing personal reports nothing', () => {
  const exposed = exposedColumns([query()], CONTRACT, ['pii', 'phi']);
  assert.deepEqual(exposed, []);
});

test('internal columns are not reported unless asked for', () => {
  const asked = exposedColumns([query()], CONTRACT, ['internal']);
  assert.equal(asked.length, 1);
  assert.deepEqual(asked[0].columns, ['order_id', 'total_amount']);
});

test('the same table across several queries is reported once', () => {
  const exposed = exposedColumns(
    [
      query({ sql: 'SELECT email FROM mirror.customers_current' }),
      query({ sql: 'SELECT first_name FROM mirror.customers_current' }),
    ],
    CONTRACT,
    ['pii'],
  );
  assert.equal(exposed.length, 1);
  assert.deepEqual(exposed[0].columns, ['email', 'first_name']);
});

test('the summary names the store and how far behind it was', () => {
  const text = summarise(record());
  assert.match(text, /Answered from mirror \(4s behind\)/);
  assert.match(text, /1 query/);
  assert.match(text, /trace abc-123456/);
});

test('a store whose freshness could not be measured says so rather than claiming zero', () => {
  const text = summarise(
    record({
      storeHealth: [
        { store: 'mirror', freshnessSeconds: null, eligible: false, reason: 'unmeasurable' },
      ],
    }),
  );
  assert.match(text, /freshness unknown/);
  assert.doesNotMatch(text, /0s behind/);
});

test('personal data is called out in the summary', () => {
  const text = summarise(
    record({
      exposed: [
        { table: 'mirror.customers_current', classification: 'pii', columns: ['email'] },
      ],
    }),
  );
  assert.match(text, /pii columns in mirror\.customers_current/);
});

test('an answer produced without querying anything says exactly that', () => {
  const text = summarise(record({ queries: [], storesQueried: [] }));
  assert.match(text, /No store was queried/);
  assert.doesNotMatch(text, /Answered from/);
});

test('a failed query still appears in the record', () => {
  const rec = record({
    queries: [query({ error: 'syntax error', rows: null })],
  });
  assert.equal(rec.queries[0].error, 'syntax error');
  assert.match(summarise(rec), /1 query/);
});
