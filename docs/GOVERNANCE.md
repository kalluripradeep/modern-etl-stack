# Governance and trust

What the platform knows about its own data, and what it does with that.

This is the layer described in #231. It exists because an agent answering
questions from three stores needs more than a connection to each: it needs to
know how fresh they are, whether anyone has checked them, what it is allowed
to read, and it has to leave a record of what it did.

## What is enforced, and what is only recorded

Worth stating first, because the difference is the whole point.

| | Mechanism | Enforced by |
|---|---|---|
| Classification is complete | CI fails on an unclassified column | the build |
| Column access | Column grants in Postgres and ClickHouse | the database |
| Freshness SLA | A store outside its SLA is not offered to the model | the application |
| Quality score | Reported to the model, never blocks | nothing |
| Provenance | A structured line per answer | nothing |

Only the first two are enforcement. The rest are signals: useful, and
defeatable by anything that does not go through the assistant.

All of them are visible over time on the **Governance and Trust** Grafana
dashboard, which is the subject of the last section.

## The contract

`airflow/dags/config/pipelines.yml` carries, per table, an `owner` and a
`classification` for every column — `public`, `internal`, `pii` or `phi`.

`scripts/ci/test_data_contract.py` checks **set equality** against the
column list, so a column nobody classified fails the build and a
classification naming a column that no longer exists fails too. A field on
its own would not have helped: `email` sat in this manifest since the repo
was written, marked `unique_columns` because dbt needed that, with nothing
recording that it identifies a person.

## Access

Columns classified `pii` or `phi` are withheld from the role the dashboard
connects as, and **the database refuses them**.

That is deliberate rather than convenient. The assistant writes its own SQL,
so a check in the application is a check against generated SQL — and an
alias (`SELECT email AS e`), an expression (`lower(email)`) or a view all
defeat matching on column names. A column grant holds however the query is
written. Verified on both stores:

| | Postgres | ClickHouse |
|---|---|---|
| `SELECT email` | denied | denied |
| `SELECT email AS e` | denied | denied |
| inside an expression | denied | denied |
| via `SELECT *` | denied | denied |
| in a `WHERE` only | denied | n/a |
| through a view | denied | denied |
| an allowed column | **allowed** | **allowed** |

Both catalogues also hide what the role cannot read: `email` does not appear
in `information_schema.columns` or `system.columns` for the restricted role,
so the assistant never learns it exists and never writes a query that fails.

### How the grants are produced

The two stores are handled differently, for a reason.

**Postgres** is introspected and re-granted at the end of every ingest run
(`apply_access_policy`). dbt creates the tables being granted on, so a model
added this run has to be covered this run. The role holds **no default
privileges** precisely so that a table nobody has classified is unreadable
rather than readable.

Denial is by column *name* across every schema the dashboard can reach. The
contract classifies four source tables; dbt then copies those columns into
`int`, `prs` and `gold` under the same names, and hand-classifying every
derived model is a list that falls behind silently. A name collision is
over-denial, which is visible and gets fixed. Under-denial is invisible.

Views are all-or-nothing: a Postgres view runs with its owner's privileges
unless declared `security_invoker`, so column grants underneath do not
protect it. Any view whose definition reads a classified column is not
granted at all — `prs.v_customers`, today.

**ClickHouse** grants are generated into `01_mirror_schema.sql` beside the
schema, because that generator owns the mirror's shape. They are granted by
exclusion, not by listing what is allowed: `ver` is what `FINAL` uses to
collapse a `ReplacingMergeTree`, and it is not in the manifest, so an
allow-list built from the contract alone makes every `<t>_current` view
unreadable. The Kafka engine tables are never granted — selecting from one
consumes the topic.

An XML-defined ClickHouse user cannot hold column grants at all, so
`docker/clickhouse/users.d/zz-access-management.xml` enables SQL access
control. The filename has to sort after the image's `default-user.xml`.

### Where this does not reach

**Trino is not protected.** The lakehouse has no column-level access control
here, so `iceberg.lake.customers` is readable in full by anything that can
reach Trino. Trino supports file-based access control and that is the route,
but it is not wired up. Until it is, the mirror and the warehouse are
governed and the lakehouse is not.

**This is not an authorisation system.** There is one restricted role and it
is the one the dashboard always uses. Nothing here authenticates a person or
varies access by who is asking; a real deployment puts an identity provider
in front and maps users to roles. What is demonstrated is that a
classification in the contract can drive enforcement in the database — not
that the platform knows who you are.

**Nor is it compliance.** Governance in a regulated setting means business
associate agreements, audit retention, breach procedures and access reviews.
This is an architecture for trusted data access. It should never be
described as more than that.

## Quality and lineage

`dbt test` results and the dbt dependency graph land in `meta.dbt_quality`
and `meta.dbt_lineage` at the end of every ingest run.

A table with no tests scores **NULL, not 1.0**. `passed / total` gives an
untested table a perfect score, which reads as *verified* and means *nobody
looked*. Five of sixteen models have no tests at all; the schema keeps
"untested" and "clean" apart, including in the column type.

Quality is readable from outside as Prometheus gauges on `/api/metrics`
(`etl_dbt_quality_score`, `etl_dbt_tests_total`, `etl_dbt_untested_tables`).
**Lineage is not.** It is written, and the assistant reads it to annotate an
answer, but nothing serves `meta.dbt_lineage` over HTTP. #231 asked for both
behind one read-only endpoint; only the quality half exists, and anything
wanting the graph has to query the table directly.

## Freshness and routing

The assistant used to choose a store from adjectives in its prompt —
"seconds fresh" for the mirror. #188 measured the mirror at 84 seconds
against a 30 second target and #225 found it a day behind a connector task
that still reported `RUNNING`.

Freshness is now measured per request using the `freshness` expression each
store declares, and a store outside its SLA is not offered. When none is
usable the model is told to say so rather than fall back to the least stale
one. See `docs/BENCHMARKING.md` for the measurement side.

### What the routing decision does not consider

The proposal in #231 named five inputs for this decision. Three are
implemented — freshness, quality and the SLA the question needs. The other two
are not, and it is worth saying which rather than leaving the gap to be
discovered:

**Cost is deliberately absent.** The proposal required it to be measured —
rows or bytes scanned, wall time — or left out entirely, on the grounds that a
routing decision justified by an invented dollar figure is worse than one that
never mentions cost. Nothing in the stack measures per-query cost today, so it
is left out. Adding it means capturing it from each engine first, not
estimating it.

**Complexity is still prompt-side.** "A question needing the star schema
favours the warehouse regardless of freshness" is the rule, and it lives in
the system prompt in `ui/src/app/api/chat/route.ts` as English, which is the
thing the rest of this layer exists to replace. It stayed there because it
depends on classifying the question before a store is chosen, and nothing in
the request path does that yet.

So the honest summary of the routing policy is: **a store too stale to trust
is withheld, and a store with poor or unknown test coverage is flagged.**
Choosing between two eligible stores on the shape of the question is still the
model's judgement, informed by prose. That is a narrower claim than #231 set
out to make, and narrower than "policy-driven engine routing" suggests on its
own.

## Provenance

Every answer emits one structured line on stdout:

```json
{"event":"ai_answer","traceId":"...","storesQueried":["mirror"],
 "queries":[{"store":"mirror","sql":"...","rows":1,"ms":146}],
 "storeHealth":[...],"exposed":[...],"answered":true}
```

Taken from the tool calls that executed, not from the model's closing
sentence about which store it used — a claim from the thing being audited is
not evidence. Every reply path records, including refusals and errors.

It is logged rather than written to the warehouse on purpose: that connection
is read-only, and an audit trail the audited component can rewrite is not an
audit trail.

The `exposed` field uses substring matching against known table names. It
over-reports, which is the safe direction for *"did this answer touch
personal data"* and the wrong direction for an access decision — which is why
the grants above are derived from the contract and the catalogue, and never
from this.

## Watching it over time

Every signal above is produced one answer at a time, which is the wrong shape
for noticing that a store has been stale all week or that denials started
last Tuesday. The dashboard exposes them at `/api/metrics` and Prometheus
scrapes it as the `etl-governance` job, in both Compose and Kubernetes.

| Metric | What it answers |
|---|---|
| `etl_store_freshness_seconds` / `etl_store_sla_seconds` | How close each store is to its limit. The ratio is the panel worth watching |
| `etl_store_eligible` | How many stores are usable right now |
| `etl_store_probe_failed` | A store that cannot report its own freshness, excluded rather than assumed healthy |
| `etl_store_sla_blocks_total` | How often staleness withheld a store from the model |
| `etl_answers_total` | Which store is actually answering |
| `etl_queries_total{outcome}` | `ok`, `error`, and **`denied` separately** |
| `etl_answer_refusals_total` | Answers the platform declined to give |
| `etl_dbt_quality_score` | Pass rate per table |
| `etl_dbt_untested_tables` | Tables nobody has tested |

`denied` is deliberately not an error. A permission error is the access
policy working, and counting it as a failure would make a correctly governed
platform look broken — and would hide a real error rate behind it.

A table with no tests is **absent** from `etl_dbt_quality_score` rather than
reported as 0 or 1. A gauge cannot say "nobody looked", so the count of such
tables is its own metric, and that is the one to alert on.

### Why the endpoint is unauthenticated

`/api/metrics` is exempt from the dashboard's basic auth, because a
Prometheus scrape job cannot hold a password without that password living in
a ConfigMap next to the scrape config.

The exemption is only defensible because of what the route emits: store
names, table names and counts. **No SQL, no column values, no question
text.** `scripts/ci/test_metrics_safety.py` holds both halves of that — the
exemption stays narrow, and the route stays incapable of carrying anything
else, checked against a live scrape when one is reachable.

Counters live in the dashboard process and reset when it restarts.
Prometheus handles counter resets, and a second replica reports its own —
fine for a signal, and not a basis for anything that needs to be exact.
