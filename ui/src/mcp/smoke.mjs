/**
 * Drives the built MCP server over a real stdio transport.
 *
 * The assertions that matter need no database: that all four tools are
 * advertised, and that the single-statement read-only gate rejects a write
 * before anything reaches a store. Those run anywhere, CI included.
 *
 * Live queries are required only when MCP_SMOKE_LIVE=1. Deciding that from the
 * error text was wrong twice over -- a connection failure can arrive with an
 * empty message, and guessing the other way would turn a broken adapter into a
 * skip. An explicit flag cannot silently pass in either mode.
 *
 * Usage: npm run build:mcp && npm run mcp:smoke
 *        MCP_SMOKE_LIVE=1 to require the stores to answer
 */
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';

const LIVE = process.env.MCP_SMOKE_LIVE === '1';
const failures = [];

const check = (ok, what) => {
  console.log(`  ${ok ? 'ok  ' : 'FAIL'}  ${what}`);
  if (!ok) failures.push(what);
};
const liveCheck = (ok, what, detail) => {
  if (ok) return check(true, what);
  if (LIVE) return check(false, `${what} -- ${detail}`);
  console.log(`  skip  ${what}: ${detail}`);
};

const client = new Client({ name: 'mcp-smoke', version: '1.0.0' });
await client.connect(new StdioClientTransport({
  command: process.execPath,
  args: ['dist/mcp/server.js'],
  env: { ...process.env },
  stderr: 'inherit',
}));

const call = (name, args) => client.callTool({ name, arguments: args });
const text = (r) => (r.content ?? []).map((c) => c.text ?? '').join('\n');
const firstLine = (r) => text(r).split('\n')[0].slice(0, 90) || '(no error message)';

const { tools } = await client.listTools();
const names = tools.map((t) => t.name).sort();
console.log(`\ntools advertised: ${names.join(', ')}`);
check(
  JSON.stringify(names) ===
    JSON.stringify(['get_schema', 'query_lakehouse', 'query_mirror', 'query_warehouse']),
  'all four tools advertised',
);
check(tools.every((t) => t.description && t.description.length > 40),
      'every tool carries a usable description');

console.log('\nread-only gate (no database needed):');
for (const [label, sql] of [
  ['a write is rejected', 'DELETE FROM raw.orders_source'],
  ['an update is rejected', "UPDATE raw.orders_source SET status='x'"],
  ['DDL is rejected', 'DROP TABLE raw.orders_source'],
  ['stacked statements are rejected', 'SELECT 1; DROP TABLE raw.orders_source'],
  ['a write hidden behind a comment is rejected', '-- SELECT 1\nDELETE FROM raw.orders_source'],
]) {
  const r = await call('query_warehouse', { sql });
  check(r.isError === true, `${label} (${firstLine(r)})`);
}

console.log(`\nlive stores (${LIVE ? 'required' : 'best effort; MCP_SMOKE_LIVE=1 to require'}):`);
const warehouse = await call('query_warehouse', { sql: 'SELECT 1 AS ok' });
liveCheck(!warehouse.isError && /\bok\b/.test(text(warehouse)),
          'warehouse answers a SELECT', firstLine(warehouse));

if (!warehouse.isError) {
  // Assert real table names, not the word WAREHOUSE. getSchemaOverview catches
  // its own errors and returns "WAREHOUSE: unavailable (...)", so matching the
  // heading passes against a dead database -- a check that cannot fail.
  const schema = text(await call('get_schema', {}));
  check(/\braw\.|\bint\.|\bgold\.|\bprs\./.test(schema),
        'get_schema lists actual warehouse tables');
  check(!/WAREHOUSE: unavailable/.test(schema),
        'get_schema does not report the warehouse as unavailable');

  for (const [tool, sql, label] of [
    ['query_lakehouse', 'SELECT count(*) AS n FROM iceberg.lake.orders', 'lakehouse'],
    ['query_mirror', 'SELECT count() AS n FROM mirror.orders_current', 'mirror'],
  ]) {
    const res = await call(tool, { sql });
    liveCheck(!res.isError && /\|/.test(text(res)), `${label} answers through MCP`, firstLine(res));
  }
}

await client.close();
console.log(failures.length ? `\nFAILED: ${failures.length}` : '\nMCP server contract: OK');
process.exit(failures.length ? 1 : 0);
