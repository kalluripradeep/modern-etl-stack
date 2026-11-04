# Reading a benchmark run

`DEPLOY_GUIDE.md` Step 5b covers how to *run* `scripts/bench/ladder.sh`. This
covers how to read what it gives back, and how to find out what a ceiling is
made of once you have one.

Every number here is from the measured runs on #188, so the worked examples
are real rather than illustrative.

## What the ladder actually exercises

pgbench writes orders into `postgres-source`, which is the common source for
all three pipelines. What happens downstream is not the same for each:

| | How it is driven | Exercised during a run? |
|---|---|---|
| **Pipe 1** — Airflow to `postgres-dest`, then dbt | `@hourly` DAG | **No**, unless an hourly run happens to collide |
| **Pipe 2** — Spark to Iceberg, queried by Trino | `@hourly`, downstream of Pipe 1's bronze parquet | **No** |
| **Pipe 3** — Debezium to Kafka to ClickHouse | Continuous streaming | **Yes** |

The freshness columns sample `mirror.orders` in ClickHouse. That is Pipe 3 and
nothing else.

So "the pipeline absorbed 2897 tps without falling behind" is a statement about
**Pipe 3 only**. Pipes 1 and 2 would meet the same rows on their next hourly
run, as a much larger increment, and this harness has never measured what that
costs. Anything written up from a ladder run should say which pipeline it is
talking about.

`scripts/benchmark.py` is the tool that compares all three, on the same rows at
the same moment. The two answer different questions.

## The columns

| Column | What it is |
|---|---|
| **arm** | One row: a single pgbench run at one fixed client count. `CLIENTS="16 32 64 128"` is four arms. Our word, not pgbench's |
| **DURATION** | Seconds *per arm* (`pgbench -T`), default 60. Four arms at 120 is 480s of load |
| **tps** | Transactions per second, as pgbench reports them |
| **lat_ms** | pgbench's `latency average`: mean wall-clock time for one whole `orders.sql` transaction — two SELECTs, two INSERTs, an UPDATE and the commit — from the client's side. Not per statement |
| **fresh_med_s** | End-to-end Pipe 3 lag in seconds: `now() - max(updated_at)` on `mirror.orders`, sampled every 5s *while the load runs*. Median over the **second half** of samples only; the first half is the pipeline absorbing the step up in load |
| **fresh_max_s** | The worst of those same steady-state samples |
| **failed** | pgbench's failed transaction count. An arm that could not start at all prints `FAILED` and the reason instead of a row of zeros |

### tps and lat_ms are not independent

`clients / latency ~= tps`. At 128 clients and 44.7ms: 128 / 0.0447 = 2864,
against a measured 2865.

That identity is why a knee looks the way it does. Past saturation the server
cannot raise tps, so every added client comes out as latency instead. Reading
the two columns together is the whole diagnosis:

```
clients   tps    lat_ms
 16        559    28.6
 32       1138    28.1     throughput doubling, latency flat     -> headroom
 64       2171    29.5     still doubling, still flat            -> headroom
128       2897    44.2     1.33x throughput for 1.50x latency    -> the knee
```

Freshness is the second, independent ceiling. `tps` flattening means Postgres
saturated; `fresh_med_s` *growing arm over arm* means Pipe 3 fell behind. They
are rarely the same arm, and measuring only the first is how a pipeline gets
called fast while its mirror quietly drifts.

## Finding what a ceiling is made of

Three measurements, in this order. Each one rules something out.

### 1. Is the container being throttled?

`kubectl top` cannot answer this. It reports a rolling average over the scrape
window, so a container pegged in bursts and idle between them reads the same as
one genuinely under its limit. At 8 cores it reported 91%, which turned out to
be both things at once.

The kernel counts throttling directly. Read it before and after a run:

```bash
kubectl exec -n etl postgres-source-0 -- sh -c 'cat /sys/fs/cgroup/cpu.stat 2>/dev/null || cat /sys/fs/cgroup/cpu/cpu.stat'
```

| Field | What it is |
|---|---|
| `usage_usec` | Total CPU consumed since the pod started, in microseconds, summed across cores. **Delta divided by wall time is the average core count used** |
| `user_usec` | The part in Postgres's own code |
| `system_usec` | The part in the kernel — syscalls, disk writes, fsyncs. A high share here is a hint on its own |
| `nr_periods` | Scheduling periods elapsed. One period is 100ms, so this doubles as a clock |
| `nr_throttled` | Periods where the quota ran out early and the container was **stopped** until the period ended |
| `throttled_usec` | Time spent throttled — but **summed per runqueue** on cgroup v2, so it can exceed wall time. Do not turn it into a percentage |

Worked example, four arms at 120s against an 8-core limit:

```
nr_periods     74,065 -> 78,825     delta 4,760 periods = 476s  (the run was 480s)
nr_throttled      928 ->  1,852     delta 924 = 19.4% of periods
usage_usec  3,396,535 -> 5,609,110  delta 2,212s CPU = 4.65 cores averaged
```

The period delta matching the run length is the check that the counters cover
this run and nothing else. Against a 1.25% lifetime baseline, 19.4% is real
throttling — while averaging 4.65 of 8 cores. That combination is exactly what
an average hides, and it is why `nr_throttled` is the number to trust.

### 2. What are the backends actually waiting on?

Run this a few times *during* an arm. It is the direct answer, not an
inference:

```bash
kubectl exec -n etl postgres-source-0 -- psql -U sourceuser -d sourcedb -tAc "select coalesce(wait_event_type,'(on cpu)'), coalesce(wait_event,'-'), count(*) from pg_stat_activity where state='active' and backend_type='client backend' group by 1,2 order by 3 desc limit 8"
```

| Reading | What it means |
|---|---|
| `(on cpu)` | Backends with no wait event — genuinely running. **If CPU is the bottleneck, this is the large number** |
| `LWLock / WALWrite` | Queued for the WAL write lock. One backend holds it through the write *and* the fsync; the rest wait |
| `IO / WALSync` | The backend doing the actual `fdatasync`. Normally exactly 1, because only the lock holder does it |
| `Client / ClientRead` | Waiting on the client, not the server. Usually pgbench, not a finding |

The measured shape at every arm:

```
 16 clients   LWLock|WALWrite  15   IO|WALSync 1   (on cpu) 1
 64 clients   LWLock|WALWrite  60   IO|WALSync 1   (on cpu) 1
128 clients   LWLock|WALWrite 127   IO|WALSync 1   (on cpu) 1
```

One backend flushing with everyone else stacked behind it is commit
serialisation, not CPU starvation. `(on cpu)` never rose above 3 at any client
count.

### 3. How fast can the disk actually commit?

```bash
kubectl exec -n etl postgres-source-0 -- pg_test_fsync -f /var/lib/postgresql/data/fsynctest
```

`pg_test_fsync` ships in the `postgres:15` image. The `fdatasync` line is the
one that matters — it is Linux's default `wal_sync_method`.

On the `local-path` volume:

```
fdatasync   207.237 ops/sec   4825 usecs/op
fsync        64.541 ops/sec  15494 usecs/op
```

207 flushes a second against 2865 tps is **about 14 commits riding every
flush**, so group commit is already doing the heavy lifting and the `WALWrite`
queue is backends waiting on the flush in progress rather than 127 separate
disk waits.

To find how much of the ceiling fsync owns, take it out of the path for one
arm:

```bash
kubectl exec -n etl postgres-source-0 -- psql -U sourceuser -d sourcedb -c "alter system set synchronous_commit=off" -c "select pg_reload_conf()"
```

and put it back afterwards:

```bash
kubectl exec -n etl postgres-source-0 -- psql -U sourceuser -d sourcedb -c "alter system reset synchronous_commit" -c "select pg_reload_conf()"
```

| | tps | lat | CPU |
|---|---|---|---|
| `synchronous_commit=on` | 2,865 | 44.7 ms | 7271m |
| `synchronous_commit=off` | 3,702 | 34.6 ms | 7962m |

1.29x — and CPU lands on 7962m of 8000m, pegged for the first time at that
limit. So at 8 cores fsync was worth 29%, and the moment it stopped binding,
cores did.

### Which of the two mattered more

Raising the CPU limit settled it. At 16 cores, with fsync back in the path:

| limit | tps | lat | CPU |
|---|---|---|---|
| 8 | 2,865 | 44.7 ms | 7271m of 8000m — 91% |
| 16 | 6,001 | 21.3 ms | 11541m of 16000m — 72% |

2.09x the throughput at **half** the latency, and the first reading with real
headroom under the limit. So CPU was the binding constraint at 8 cores after
all, and the queue on `LWLock:WALWrite` was substantially a symptom of it — a
backend that is throttled while holding the lock stalls every backend behind
it.

The reason 207 flushes a second never capped throughput is **group commit**.
The batch grows with concurrency: 14 commits rode each flush at 2,865 tps, 29
at 6,001. The fsync rate sets a floor on commit latency, not a ceiling on
throughput, which is why reasoning from `ops/sec` straight to a tps limit gets
it wrong.

`synchronous_commit=off` is a measurement, not a setting this stack runs. It
trades the last `wal_writer_delay` of commits on a crash — fine on a benchmark
source, not on a real one. It does not break Debezium: logical decoding still
reads flushed WAL, the flush just moves to the WAL writer.

## Things that have misled us

- **`kubectl top` is an average.** It cannot distinguish bursty throttling from
  a genuine sub-limit ceiling. Use `nr_throttled`.
- **`throttled_usec` is summed per runqueue.** 117s over a 476s run is not 25%
  of wall clock.
- **A long `LWLock:WALWrite` queue is not proof that WAL bounds the rate.**
  Backends queue there whenever commits are serialised, including when the
  backend holding the lock is itself being throttled. Read it together with
  `nr_throttled`, and with what happens when the CPU limit moves.
- **A stopped pipeline does not read as zero freshness.** `max(updated_at)`
  simply stops moving, so lag climbs at one second per second and lands in the
  table as an ordinary-looking number. `ladder.sh` proves the mirror is live
  before measuring anything against it, and aborts if it is not.
- **That liveness check proves a write lands, not that the mirror has caught
  up.** A run started immediately after a failed Connect task is restarted can
  report the tail of the backlog as steady-state freshness. Let the mirror
  settle first, or re-run the arm before believing a freshness number that
  jumped.
- **The row picker is part of the measurement.** `orders.sql` picks its rows
  with `ORDER BY random() LIMIT 1` on `customers` and `products`, twice per
  transaction. Its comment calls that "noise next to the write", which was
  written when the source was doing 212 tps. At 2897 it is around 5,800 sorts
  a second. It is not what bounds the rate — those backends would show as
  `(on cpu)` — but it is a tax on the measurement.
