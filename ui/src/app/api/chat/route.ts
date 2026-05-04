import { NextResponse } from 'next/server';
import { Client } from 'pg';
import Anthropic from '@anthropic-ai/sdk';

// --- CONFIGURATION ---
const SCHEMA_CONTEXT = `
You are a Senior Data Analyst for a Modern ETL Stack. 
The database is PostgreSQL. You have access to the following tables:

1. public.orders (order_id, customer_id, order_date, total_amount, status)
2. public.order_items (item_id, order_id, product_id, quantity, unit_price)
3. public.products (product_id, name, price, category)
4. public.customers (customer_id, first_name, last_name, email, city)

RULES:
- Always use COUNT(DISTINCT order_id) for order counts.
- For revenue, use SUM(quantity * unit_price) from order_items joined to orders.
- Output your response in two parts:
  1. A brief analytical observation.
  2. The SQL query you want to run inside a <query>...</query> tag.
- If the user asks a non-data question, answer politely as a Data Assistant.
`;

export async function POST(request: Request) {
  const { query } = await request.json();
  const apiKey = process.env.ANTHROPIC_API_KEY;
  
  // --- FALLBACK: SIMULATED AI (DEMO MODE) ---
  if (!apiKey || apiKey === 'YOUR_API_KEY_HERE') {
    return handleSimulatedResponse(query);
  }

  // --- PRODUCTION: REAL CLAUDE BRAIN ---
  const anthropic = new Anthropic({ apiKey });
  const dbClient = new Client({
    user: process.env.DEST_DB_USER || 'destuser',
    host: process.env.DEST_DB_HOST || 'localhost',
    database: process.env.DEST_DB_NAME || 'destdb',
    password: process.env.DEST_DB_PASSWORD || 'destpass',
    port: parseInt(process.env.DEST_DB_PORT || '5433'),
  });

  try {
    // 1. Ask Claude to generate the SQL
    const msg = await anthropic.messages.create({
      model: "claude-3-5-sonnet-20241022",
      max_tokens: 1024,
      system: SCHEMA_CONTEXT,
      messages: [{ role: "user", content: query }],
    });

    const content = (msg.content[0] as any).text;
    const sqlMatch = content.match(/<query>([\s\S]*?)<\/query>/);

    if (!sqlMatch) {
      return NextResponse.json({ role: 'assistant', content });
    }

    const sql = sqlMatch[1].trim();

    // 2. Execute the SQL against the Live Warehouse
    await dbClient.connect();
    const dbRes = await dbClient.query(sql);
    
    // 3. Format the result for the UI
    const rows = dbRes.rows;
    const tableHeader = rows.length > 0 ? `| ${Object.keys(rows[0]).join(' | ')} |` : '';
    const tableDivider = rows.length > 0 ? `| ${Object.keys(rows[0]).map(() => '---').join(' | ')} |` : '';
    const tableRows = rows.map((r: any) => `| ${Object.values(r).join(' | ')} |`).join('\n');

    return NextResponse.json({
      role: 'assistant',
      content: `${content.split('<query>')[0]}

${tableHeader}
${tableDivider}
${tableRows}

---
**Agentic SQL Executed:**
` + "```sql" + `
${sql}
` + "```" + ``,
    });

  } catch (err: any) {
    console.error(err);
    return NextResponse.json({ role: 'assistant', content: `⚠️ **AI Brain Error:** ${err.message}` });
  } finally {
    await dbClient.end();
  }
}

// --- DEMO MODE HANDLER (The "Hardcoded" version) ---
async function handleSimulatedResponse(q: string) {
  const lowerQ = q.toLowerCase();
  
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

*(Note: To enable Real AI reasoning, add an Anthropic API key to the .env file)*`
    });
  }

  return NextResponse.json({
    role: 'assistant',
    content: "I'm currently in **Demo Mode**. I can answer questions about orders, revenue, and trends. \n\n*To unlock my full potential, please provide an API key in the backend configuration.*"
  });
}
