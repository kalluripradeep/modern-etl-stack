# AI Data Assistant (Dashboard UI)

This is a [Next.js](https://nextjs.org) project bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## Getting Started

First, run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

You can start editing the page by modifying `app/page.tsx`. The page auto-updates as you edit the file.

This project uses [`next/font`](https://nextjs.org/docs/app/building-your-application/optimizing/fonts) to automatically optimize and load [Geist](https://vercel.com/font), a new font family for Vercel.

## MCP Server

`src/mcp/server.ts` exposes the platform's three data stores over the Model
Context Protocol, so Claude Code, Claude Desktop or any MCP client can query
the warehouse, the Iceberg lakehouse and the CDC mirror directly.

It is an adapter over `src/lib/datastores.ts` — the same module the dashboard
uses — so both clients inherit one set of rules: a single-statement
SELECT-only gate, read-only enforced server-side by each store, a 20s
statement timeout and a 100-row cap. Reimplementing those for MCP would mean
two copies of the rules that stop an LLM writing to the warehouse, and they
would drift.

| Tool | Store | Dialect |
|---|---|---|
| `get_schema` | all three | — |
| `query_warehouse` | PostgreSQL (`gold`/`prs`/`int`/`raw`) | PostgreSQL |
| `query_lakehouse` | Iceberg via Trino (`iceberg.lake.*`) | Trino |
| `query_mirror` | ClickHouse (`mirror.*_current`) | ClickHouse |

Not to be confused with [dbt-mcp](https://docs.getdbt.com/docs/dbt-ai/about-mcp),
which exposes dbt itself (run, test, semantic layer). That is a separate
upstream server and installs alongside (`uvx dbt-mcp`); this one exposes the
stores.

### Build and check

```bash
npm run build:mcp     # tsc -p tsconfig.mcp.json -> dist/
npm run mcp:smoke     # drives the server over a real stdio transport
```

`mcp:smoke` always asserts the tool list and the read-only gate, neither of
which needs a database. Set `MCP_SMOKE_LIVE=1` to also require all three
stores to answer — without it, unreachable stores are reported as skips rather
than counted as passes.

### Wire it into a client

Claude Code:

```bash
claude mcp add modern-etl-stack -- node /absolute/path/to/ui/dist/mcp/server.js
```

Claude Desktop, or any client using a JSON config:

```json
{
  "mcpServers": {
    "modern-etl-stack": {
      "command": "node",
      "args": ["/absolute/path/to/ui/dist/mcp/server.js"],
      "env": {
        "DEST_DB_HOST": "localhost",
        "DEST_DB_PORT": "5433",
        "DEST_DB_NAME": "destdb",
        "DEST_DB_USER": "dashboard_ro",
        "DEST_DB_PASSWORD": "...",
        "TRINO_URL": "http://localhost:8082",
        "CLICKHOUSE_URL": "http://localhost:8123",
        "CLICKHOUSE_USER": "chuser",
        "CLICKHOUSE_PASSWORD": "..."
      }
    }
  }
}
```

Use `dashboard_ro` rather than `destuser`: the gate in front of the SQL is one
layer, and a SELECT-only role is the one that holds if the gate is ever wrong.
`TRINO_URL` and `CLICKHOUSE_URL` are optional — omit either and its tool
reports what is missing instead of failing obscurely. Ports above are the
docker-compose ones; on Kubernetes, port-forward first.

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

You can check out [the Next.js GitHub repository](https://github.com/vercel/next.js) - your feedback and contributions are welcome!

## Deploy on Vercel

The easiest way to deploy your Next.js app is to use the [Vercel Platform](https://vercel.com/new?utm_medium=default-template&filter=next.js&utm_source=create-next-app&utm_campaign=create-next-app-readme) from the creators of Next.js.

Check out our [Next.js deployment documentation](https://nextjs.org/docs/app/building-your-application/deploying) for more details.
