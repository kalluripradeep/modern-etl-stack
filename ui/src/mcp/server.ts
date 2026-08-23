#!/usr/bin/env node
/**
 * MCP server exposing the platform's three data stores as tools.
 *
 * Every guarantee here comes from ui/src/lib/datastores.ts, which the AI
 * dashboard already uses: a single-statement SELECT gate on the way in,
 * read-only enforced server-side by each store, a statement timeout, and a
 * row cap. This file is an adapter, deliberately -- reimplementing any of
 * that for MCP would mean two copies of the rules that keep an LLM from
 * writing to the warehouse, and they would drift.
 *
 * The dashboard is one client of those tools over HTTP. This is the other,
 * over MCP, so Claude Code, Claude Desktop or any MCP client can query the
 * warehouse, the lakehouse and the CDC mirror directly.
 *
 * Not to be confused with dbt-mcp, which exposes dbt itself (run, test,
 * semantic layer). That is a separate upstream server and installs alongside
 * (`uvx dbt-mcp`); this one exposes the three stores.
 *
 * Run:  npm run mcp   (after npm run build:mcp)
 * Wire it into a client with the config in ui/README.md.
 */

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';

import {
  clickhouseConfigured,
  getSchemaOverview,
  queryLakehouse,
  queryMirror,
  queryWarehouse,
  resultToMarkdown,
  trinoConfigured,
  type SqlResult,
} from '../lib/datastores';

const server = new McpServer({
  name: 'modern-etl-stack',
  version: '1.0.0',
});

/** Tool errors go back to the model as content, not as a transport failure.
 *  A rejected query is information it can act on -- fix the SQL and retry --
 *  which is exactly how the dashboard's own retry loop already behaves. */
async function respond(run: () => Promise<SqlResult>) {
  try {
    return { content: [{ type: 'text' as const, text: resultToMarkdown(await run()) }] };
  } catch (err) {
    return {
      isError: true,
      content: [{
        type: 'text' as const,
        text: `Error: ${err instanceof Error ? err.message : String(err)}`,
      }],
    };
  }
}

const sqlArg = {
  sql: z.string().describe('A single SELECT (or WITH ... SELECT) statement'),
};

server.registerTool(
  'get_schema',
  {
    description:
      'List every queryable table and its columns across the warehouse, the ' +
      'Iceberg lakehouse and the ClickHouse mirror. Call this first when ' +
      'unsure of table or column names. Stores that are not configured are ' +
      'reported as unavailable rather than omitted silently.',
    inputSchema: {},
  },
  async () => ({ content: [{ type: 'text' as const, text: await getSchemaOverview() }] }),
);

server.registerTool(
  'query_warehouse',
  {
    description:
      'Run one read-only statement on the PostgreSQL analytics warehouse ' +
      '(PostgreSQL dialect). Star schema in gold.* (fact_order_items plus ' +
      'dim_customer / dim_product / dim_date), curated views in prs.*, ' +
      'intermediate dbt layers in int.* and raw.*. Batch freshness.',
    inputSchema: sqlArg,
  },
  async ({ sql }) => respond(() => queryWarehouse(sql)),
);

server.registerTool(
  'query_lakehouse',
  {
    description:
      'Run one read-only statement on the Iceberg lakehouse through Trino ' +
      '(Trino dialect). Tables live under iceberg.lake.* — prefix them, e.g. ' +
      'iceberg.lake.orders. Use for large historical scans. Requires ' +
      'TRINO_URL.',
    inputSchema: sqlArg,
  },
  async ({ sql }) => respond(() => queryLakehouse(sql)),
);

server.registerTool(
  'query_mirror',
  {
    description:
      'Run one read-only statement on the ClickHouse CDC mirror (ClickHouse ' +
      'dialect). Query the mirror.*_current views, which collapse the ' +
      'ReplacingMergeTree and drop deleted rows — the base tables will ' +
      'otherwise show superseded versions. Seconds-fresh; use for any ' +
      '"right now" question. Requires CLICKHOUSE_URL.',
    inputSchema: sqlArg,
  },
  async ({ sql }) => respond(() => queryMirror(sql)),
);

async function main() {
  // stderr, not stdout: stdout carries the JSON-RPC framing and anything
  // else written there corrupts the stream.
  const optional = [
    ['lakehouse (TRINO_URL)', trinoConfigured()],
    ['mirror (CLICKHOUSE_URL)', clickhouseConfigured()],
  ] as const;
  for (const [name, ready] of optional) {
    if (!ready) {
      console.error(`[modern-etl-stack] ${name} not configured; its tool will report why when called.`);
    }
  }

  await server.connect(new StdioServerTransport());
  console.error('[modern-etl-stack] MCP server ready on stdio.');
}

main().catch((err) => {
  console.error('[modern-etl-stack] failed to start:', err);
  process.exit(1);
});
