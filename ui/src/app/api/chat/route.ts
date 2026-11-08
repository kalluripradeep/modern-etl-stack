import { NextResponse } from 'next/server';
import Anthropic from '@anthropic-ai/sdk';
import {
  getSchemaOverview,
  queryLakehouse,
  queryMirror,
  queryWarehouse,
  resultToMarkdown,
} from '@/lib/datastores';
import { STORES } from '@/lib/stores.generated';
import {
  measureStores,
  describeStores,
  contractByPhysicalTable,
} from '@/lib/store-health';
import {
  newTraceId,
  exposedColumns,
  summarise,
  type QueryRecord,
  type ProvenanceRecord,
} from '@/lib/provenance';

const MAX_TOOL_ITERATIONS = 8;

// One constant rather than a literal at each messages.create call. The two
// were identical only by habit, and the loop below re-sends the model on
// every tool round trip -- a bump that updated one of them would have run
// two different models in a single conversation.
//
// Switching tiers is a one-word change here. Opus suits the harder questions
// this answers, where a wrong join is worse than a slow reply; Sonnet is the
// cheaper, faster choice if the dashboard is mostly answering simple ones.
const MODEL = 'claude-opus-5-5';

// Table names in these descriptions come from the generated store registry
// rather than being spelled out again here. The model is told which tables
// exist, so a name typed from memory is a name it will confidently query and
// the database will reject -- and this file, being the one consumer written
// in TypeScript, is exactly where the naming convention drifted before.
function tableList(store: keyof typeof STORES): string {
  return Object.values(STORES[store].tables).join(', ');
}

// Interpolated rather than written out, for the same reason the tool
// descriptions are: every physical table name in this file was a fourth copy
// of a convention the manifest already owns.
const SYSTEM_PROMPT = `
You are the data analyst for a Modern ETL Stack. You answer questions by
querying one of three read-only data stores, each with its own tool:

1. query_warehouse — PostgreSQL analytics warehouse (batch).
   - gold.fact_order_items + gold.dim_customer / dim_product / dim_date:
     star schema. Prefer this for anything sliced by customer, product or
     time — join the fact to the dimensions on customer_id, product_id and
     order_date_day. Measures (quantity, line_amount) are additive; order
     totals are sum(line_amount) grouped by order_id.
   - prs.v_daily_revenue / v_orders / v_customers / v_products: curated dbt
     views. Convenient for simple headline metrics.
   - int.*_clean and raw.*_source: intermediate dbt layers.
   - meta.dbt_quality: one row per table with tests_passed/tests_total from
     the last dbt run. **score is NULL when a table has no tests** -- that
     means untested, not perfect, and must never be reported as healthy.
   - meta.dbt_lineage: child/parent edges between those tables.
     Use these two when asked how trustworthy a number is or where it
     came from.
2. query_lakehouse — Trino over Apache Iceberg (historical silver layer).
   Tables: ${tableList('lakehouse')}. Trino SQL dialect. Use for large
   historical scans.
3. query_mirror — ClickHouse, a CDC mirror of the operational tables.
   Tables: ${tableList('mirror')}. Normally the freshest store, but how fresh
   it is right now is measured and given to you below — never assume it.

Rules:
- Call get_schema first when you are unsure about table or column names.
- One SELECT (or WITH ... SELECT) per call; writes are impossible.
- Always add a LIMIT to exploratory queries.
- If a query errors, read the error and try a corrected query.
- Finish with a concise answer: a short observation, the result as a
  markdown table when tabular, and the SQL you ran in a \`\`\`sql block.
- For non-data questions, answer briefly as a data assistant without tools.

Freshness is measured before every question and appears under STORE HEALTH
below. It replaces any assumption about which store is current.
- Never answer a question about the present from a store marked DO NOT USE.
- If no store is usable, say so and give the measured lag. Do not quietly
  fall back to the least stale one and present its figures as current.
- Close with one short line naming the store you used and how far behind it
  was.
`;

function lakehouseDescription(): string {
  return (
    'Run a single read-only SQL statement on the Iceberg lakehouse via Trino '
    + `(Trino dialect). Tables: ${tableList('lakehouse')}`
  );
}

function mirrorDescription(): string {
  return (
    'Run a single read-only SQL statement on the ClickHouse columnar mirror '
    + `(ClickHouse dialect). Tables: ${tableList('mirror')}`
  );
}

const TOOLS: Anthropic.Tool[] = [
  {
    name: 'get_schema',
    description: 'List every queryable table and its columns across the warehouse, lakehouse, and columnar mirror. Call this before querying when unsure of names.',
    input_schema: { type: 'object' as const, properties: {}, additionalProperties: false },
  },
  {
    name: 'query_warehouse',
    description: 'Run a single read-only SQL statement on the PostgreSQL analytics warehouse (PostgreSQL dialect). Star schema in gold.* (fact_order_items + dim_*), curated views in prs.*, intermediate layers in int.* and raw.*.',
    input_schema: {
      type: 'object' as const,
      properties: { sql: { type: 'string' as const, description: 'A single SELECT statement' } },
      required: ['sql'],
      additionalProperties: false,
    },
  },
  {
    name: 'query_lakehouse',
    description: lakehouseDescription(),
    input_schema: {
      type: 'object' as const,
      properties: { sql: { type: 'string' as const, description: 'A single SELECT statement' } },
      required: ['sql'],
      additionalProperties: false,
    },
  },
  {
    name: 'query_mirror',
    description: mirrorDescription(),
    input_schema: {
      type: 'object' as const,
      properties: { sql: { type: 'string' as const, description: 'A single SELECT statement' } },
      required: ['sql'],
      additionalProperties: false,
    },
  },
];

const TOOL_STORES: Record<string, string> = {
  query_warehouse: 'warehouse',
  query_lakehouse: 'lakehouse',
  query_mirror: 'mirror',
};

/**
 * Runs one tool call and appends what it did to `log`.
 *
 * The record is taken here, from the call that executed, rather than from the
 * model's closing sentence about which store it used. The latter is generated
 * text about the query; this is the query (#231 phase 3).
 */
async function executeTool(
  name: string,
  input: unknown,
  log: QueryRecord[],
): Promise<string> {
  const sql = (input as { sql?: string })?.sql ?? '';
  const store = TOOL_STORES[name];
  const started = Date.now();

  try {
    if (name === 'get_schema') return await getSchemaOverview();

    let result;
    switch (name) {
      case 'query_warehouse': result = await queryWarehouse(sql); break;
      case 'query_lakehouse': result = await queryLakehouse(sql); break;
      case 'query_mirror': result = await queryMirror(sql); break;
      default: return `Unknown tool: ${name}`;
    }
    log.push({ store, sql, rows: result.rowCount, ms: Date.now() - started });
    return resultToMarkdown(result);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    // A rejected query is part of the record. An audit trail that keeps only
    // the successes describes a different session than the one that happened.
    if (store) {
      log.push({ store, sql, rows: null, ms: Date.now() - started, error: message });
    }
    // Surface the error to the model so it can correct the query
    throw new Error(message);
  }
}


/**
 * Assemble the record, write it where it will outlive the process, and hand
 * back what the reply should carry.
 *
 * Deliberately not written to the warehouse. That connection is read-only by
 * design, and an audit trail the audited component can rewrite is not an
 * audit trail. One structured line on stdout is what a cluster's log pipeline
 * already collects, and it leaves the assistant unable to edit its own
 * history.
 */
function recordProvenance(
  traceId: string,
  question: string,
  queryLog: QueryRecord[],
  verdicts: { store: string; freshnessSeconds: number | null; eligible: boolean; reason: string }[],
  answered: boolean,
): ProvenanceRecord {
  const record: ProvenanceRecord = {
    traceId,
    at: new Date().toISOString(),
    question,
    storesQueried: [...new Set(queryLog.map((q) => q.store))].sort(),
    queries: queryLog,
    storeHealth: verdicts.map((v) => ({
      store: v.store,
      freshnessSeconds: v.freshnessSeconds,
      eligible: v.eligible,
      reason: v.reason,
    })),
    // Over-reports rather than under-reports: a table named inside a string
    // literal counts. For "did this answer touch personal data" that is the
    // safe direction, and it is why this does not drive masking.
    exposed: exposedColumns(queryLog, contractByPhysicalTable(), ['pii', 'phi']),
    answered,
  };

  console.log(JSON.stringify({ event: 'ai_answer', ...record }));
  return record;
}

export async function POST(request: Request) {
  const { query } = await request.json();
  const apiKey = process.env.ANTHROPIC_API_KEY;

  if (!apiKey || apiKey === 'YOUR_API_KEY_HERE') {
    return handleSimulatedResponse(query);
  }

  const anthropic = new Anthropic({ apiKey });
  const messages: Anthropic.MessageParam[] = [{ role: 'user', content: query }];

  // Measured before the model sees the question, so the choice of store rests
  // on evidence rather than on the adjectives that used to be in the prompt
  // above. #188 measured the mirror at 84s against a 30s SLA and #225 found
  // it a day behind; both would have read as "seconds fresh".
  const verdicts = await measureStores();
  const traceId = newTraceId();
  const queryLog: QueryRecord[] = [];
  const system = `${SYSTEM_PROMPT}

STORE HEALTH
${describeStores(verdicts)}`;

  try {
    let response = await anthropic.messages.create({
      model: MODEL,
      max_tokens: 3000,
      system,
      tools: TOOLS,
      messages,
    });

    for (let i = 0; i < MAX_TOOL_ITERATIONS && response.stop_reason === 'tool_use'; i++) {
      const toolUses = response.content.filter(
        (b): b is Anthropic.ToolUseBlock => b.type === 'tool_use',
      );
      messages.push({ role: 'assistant', content: response.content });

      const results: Anthropic.ToolResultBlockParam[] = await Promise.all(
        toolUses.map(async (tu) => {
          try {
            return {
              type: 'tool_result' as const,
              tool_use_id: tu.id,
              content: await executeTool(tu.name, tu.input, queryLog),
            };
          } catch (err) {
            return {
              type: 'tool_result' as const,
              tool_use_id: tu.id,
              content: `Error: ${err instanceof Error ? err.message : err}`,
              is_error: true,
            };
          }
        }),
      );
      messages.push({ role: 'user', content: results });

      response = await anthropic.messages.create({
        model: MODEL,
        max_tokens: 3000,
        system,
        tools: TOOLS,
        messages,
      });
    }

    if (response.stop_reason === 'refusal') {
      recordProvenance(traceId, query, queryLog, verdicts, false);
      return NextResponse.json({
        role: 'assistant',
        content: 'I can\'t help with that request.',
      });
    }

    const text = response.content
      .filter((b): b is Anthropic.TextBlock => b.type === 'text')
      .map((b) => b.text)
      .join('\n')
      .trim();

    const record = recordProvenance(traceId, query, queryLog, verdicts, Boolean(text));
    return NextResponse.json({
      role: 'assistant',
      content: text || 'I ran out of steps while researching that — try a more specific question.',
      provenance: record,
      provenanceSummary: summarise(record),
    });
  } catch (err) {
    console.error(err);
    // A failed answer is still a thing that happened, and the queries that
    // ran before it failed still ran.
    recordProvenance(traceId, query, queryLog, verdicts, false);
    const message = err instanceof Error ? err.message : 'Unknown error';
    return NextResponse.json({ role: 'assistant', content: `⚠️ **AI Brain Error:** ${message}` });
  }
}

// --- DEMO MODE HANDLER (no API key configured) ---
async function handleSimulatedResponse(q: string) {
  const lowerQ = (q || '').toLowerCase();

  if (lowerQ.includes('order') || lowerQ.includes('how many')) {
    return NextResponse.json({
      role: 'assistant',
      content: `### 🧪 Demo Mode (Simulated AI)
I've scanned the **Silver Layer**. There are currently **11,000 total orders** in the system.

| Status | Count |
| :--- | :--- |
| Shipped | 2,450 |
| Delivered | 2,100 |
| Cancelled | 1,900 |
| Pending | 4,550 |

*(Note: To enable Real AI reasoning, add an Anthropic API key to the .env file)*`,
    });
  }

  return NextResponse.json({
    role: 'assistant',
    content: "I'm currently in **Demo Mode**. I can answer questions about orders, revenue, and trends. \n\n*To unlock my full potential, please provide an API key in the backend configuration.*",
  });
}
