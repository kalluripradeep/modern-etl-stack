import { NextResponse } from 'next/server';
import Anthropic from '@anthropic-ai/sdk';
import {
  getSchemaOverview,
  queryLakehouse,
  queryMirror,
  queryWarehouse,
  resultToMarkdown,
} from '@/lib/datastores';

const MAX_TOOL_ITERATIONS = 8;

const SYSTEM_PROMPT = `
You are the data analyst for a Modern ETL Stack. You answer questions by
querying one of three read-only data stores, each with its own tool:

1. query_warehouse — PostgreSQL analytics warehouse (batch, daily fresh).
   - gold.fact_order_items + gold.dim_customer / dim_product / dim_date:
     star schema. Prefer this for anything sliced by customer, product or
     time — join the fact to the dimensions on customer_id, product_id and
     order_date_day. Measures (quantity, line_amount) are additive; order
     totals are sum(line_amount) grouped by order_id.
   - prs.v_daily_revenue / v_orders / v_customers / v_products: curated dbt
     views. Convenient for simple headline metrics.
   - int.*_clean and raw.*_source: intermediate dbt layers.
2. query_lakehouse — Trino over Apache Iceberg (historical silver layer).
   Tables live under iceberg.lake.* (e.g. iceberg.lake.orders). Trino SQL
   dialect. Use for large historical scans.
3. query_mirror — ClickHouse, the LIVE real-time mirror of the operational
   tables (seconds fresh, via CDC). Use mirror.orders_current /
   customers_current / products_current / order_items_current for any
   "right now" question and for fast aggregations over live data.

Rules:
- Call get_schema first when you are unsure about table or column names.
- One SELECT (or WITH ... SELECT) per call; writes are impossible.
- Always add a LIMIT to exploratory queries.
- If a query errors, read the error and try a corrected query.
- Finish with a concise answer: a short observation, the result as a
  markdown table when tabular, and the SQL you ran in a \`\`\`sql block.
- For non-data questions, answer briefly as a data assistant without tools.
`;

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
    description: 'Run a single read-only SQL statement on the Iceberg lakehouse via Trino (Trino dialect). Tables: iceberg.lake.*',
    input_schema: {
      type: 'object' as const,
      properties: { sql: { type: 'string' as const, description: 'A single SELECT statement' } },
      required: ['sql'],
      additionalProperties: false,
    },
  },
  {
    name: 'query_mirror',
    description: 'Run a single read-only SQL statement on the ClickHouse columnar mirror (ClickHouse dialect). Tables: mirror.*_current views.',
    input_schema: {
      type: 'object' as const,
      properties: { sql: { type: 'string' as const, description: 'A single SELECT statement' } },
      required: ['sql'],
      additionalProperties: false,
    },
  },
];

async function executeTool(name: string, input: unknown): Promise<string> {
  const sql = (input as { sql?: string })?.sql ?? '';
  try {
    switch (name) {
      case 'get_schema':
        return await getSchemaOverview();
      case 'query_warehouse':
        return resultToMarkdown(await queryWarehouse(sql));
      case 'query_lakehouse':
        return resultToMarkdown(await queryLakehouse(sql));
      case 'query_mirror':
        return resultToMarkdown(await queryMirror(sql));
      default:
        return `Unknown tool: ${name}`;
    }
  } catch (err) {
    // Surface the error to the model so it can correct the query
    throw new Error(err instanceof Error ? err.message : String(err));
  }
}

export async function POST(request: Request) {
  const { query } = await request.json();
  const apiKey = process.env.ANTHROPIC_API_KEY;

  if (!apiKey || apiKey === 'YOUR_API_KEY_HERE') {
    return handleSimulatedResponse(query);
  }

  const anthropic = new Anthropic({ apiKey });
  const messages: Anthropic.MessageParam[] = [{ role: 'user', content: query }];

  try {
    let response = await anthropic.messages.create({
      model: 'claude-opus-4-8',
      max_tokens: 3000,
      system: SYSTEM_PROMPT,
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
              content: await executeTool(tu.name, tu.input),
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
        model: 'claude-opus-4-8',
        max_tokens: 3000,
        system: SYSTEM_PROMPT,
        tools: TOOLS,
        messages,
      });
    }

    if (response.stop_reason === 'refusal') {
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

    return NextResponse.json({
      role: 'assistant',
      content: text || 'I ran out of steps while researching that — try a more specific question.',
    });
  } catch (err) {
    console.error(err);
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
