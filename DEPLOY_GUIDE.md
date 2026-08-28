# ETL Stack — Deployment Guide

This guide walks you through deploying the full ETL stack on your Kubernetes cluster.

---

## What you are deploying

One source database feeding three parallel pipelines, plus a query layer:

- **PostgreSQL** — source (transactions) and destination (batch analytics warehouse)
- **Kafka (Strimzi) + Debezium** — real-time change data capture (CDC)
- **ClickHouse** — columnar real-time mirror fed from the CDC stream
- **MinIO** — S3-compatible object storage (bronze + silver layers)
- **Apache Spark + Iceberg** — large-scale lakehouse transformation
- **Trino** — SQL query engine over the Iceberg lakehouse (official Helm chart)
- **Apache Airflow** — pipeline orchestration (official Helm chart)
- **AI Dashboard** — natural-language assistant over all three stores
- **Prometheus + Grafana + Alertmanager** — monitoring, dashboards, alerting

---

## Prerequisites

Install these tools on your machine before starting.

### 1. kubectl

```bash
# Mac
brew install kubectl

# Windows (run in PowerShell as Admin)
choco install kubernetes-cli

# Linux
curl -LO "https://dl.k8s.io/release/$(curl -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
chmod +x kubectl && sudo mv kubectl /usr/local/bin/
```

Verify: `kubectl version --client`

### 2. Helm

```bash
# Mac
brew install helm

# Windows
choco install kubernetes-helm

# Linux
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

Verify: `helm version`

### 3. Docker

Download and install from: **<https://docs.docker.com/get-docker/>**

Verify: `docker --version`

### 4. Docker Hub account (free)

Needed to push the custom Airflow and dashboard images. Sign up at **<https://hub.docker.com>**, then:

```bash
docker login
```

### 5. Kubernetes cluster access

```bash
kubectl cluster-info
```

You should see your cluster URL, not an error. If this fails, ask whoever manages your cluster for the kubeconfig file.

---

## Deployment Steps

### Step 1 — Clone the repository

```bash
git clone https://github.com/kalluripradeep/modern-etl-stack.git
cd modern-etl-stack
```

### Step 2 — Generate credentials (recommended)

```bash
bash k8s/generate-secrets.sh
```

This writes `k8s/01-secrets.generated.yaml` (gitignored) with random passwords, and the deploy script picks it up automatically. **Save the printed passwords** — you need them for the dashboards in Step 6. Skipping this deploys well-known default credentials, acceptable only for a throwaway cluster.

**Optional — enable the real AI assistant:** the AI dashboard runs in demo mode unless it has an Anthropic API key. Add it to the secret at any time (before or after deploying):

```bash
kubectl -n etl patch secret etl-secrets -p '{"stringData":{"ANTHROPIC_API_KEY":"sk-ant-..."}}'
kubectl -n etl rollout restart deployment/data-dashboard
```

### Step 3 — Run the deploy script

```bash
bash k8s/deploy.sh
```

The script asks three questions:

1. **Registry** — type your Docker Hub username, e.g. `docker.io/yourname` (or press Enter to skip image builds — DAGs and dashboard will be missing).
2. **StorageClass** — the script auto-detects your cluster's default and suggests it; press Enter to accept. No manual file editing needed.
3. **Seed sample data?** — type `y` to load sample e-commerce data into the source database.

It then deploys everything: databases, Strimzi Kafka + Debezium connector, ClickHouse, MinIO (with bronze/silver buckets), Spark, Trino (Helm), Airflow (Helm), the AI dashboard, and the monitoring stack. First run takes about **10–15 minutes** (Kafka cluster startup is the slow part).

### Step 4 — Wait for all pods to be Running

```bash
kubectl get pods -n etl -w
```

Wait until every pod shows `Running`/`Completed`, then Ctrl+C. Expect roughly:

```text
NAME                                READY   STATUS
airflow-api-server-xxx              1/1     Running
airflow-scheduler-xxx               1/1     Running
airflow-dag-processor-xxx           1/1     Running
alertmanager-xxx                    1/1     Running
clickhouse-0                        1/1     Running
data-dashboard-xxx                  1/1     Running
etl-kafka-dual-role-0               1/1     Running
grafana-xxx                         1/1     Running
kafka-connect-0                     1/1     Running
kafka-exporter-xxx                  1/1     Running
kafka-ui-xxx                        1/1     Running
minio-0                             1/1     Running
postgres-source-0                   1/1     Running
postgres-dest-0                     1/1     Running
prometheus-xxx                      1/1     Running
spark-master-0                      1/1     Running
spark-worker-xxx                    1/1     Running
strimzi-cluster-operator-xxx        1/1     Running
trino-coordinator-xxx               1/1     Running
trino-worker-xxx (x2)               1/1     Running
```

### Step 5 — Run the end-to-end test

```bash
bash scripts/test_e2e.sh
```

This port-forwards the services, seeds 200 orders, **triggers
`ingest_source_to_bronze` and waits**, fires live transactions (updates,
cancellations, hard deletes), verifies the ClickHouse mirror caught every
change, **triggers `spark_transform_silver` and waits**, then reads the Iceberg
lakehouse through Trino and checks the dbt marts — one assertion per pipeline.

Both DAGs are triggered rather than re-implemented. The test used to extract and
write its own Bronze parquet, which put a second writer in the DAG's own prefix:
files named without a run stamp, so each run overwrote the last, and a different
type inference to the DAG's, so Spark refused the whole prefix with
`SchemaColumnConvertNotSupportedException: column: [order_id]`. One writer avoids
the class.

The Spark trigger matters. Without it the lakehouse check asserted a result
nothing in the test causes: the silver DAG runs on its own hourly schedule, so
on any cluster where `iceberg.lake.orders` did not already exist the check
failed however healthy Pipe 2 was — reporting a broken pipeline when the truth
was an unfinished one. Waiting can take a few minutes; `SILVER_DAG_TIMEOUT`
(default 900s) bounds it.

If runs are already queued or running, the step waits for those instead of
triggering another. The DAG is `max_active_runs=1`, so a new run joins the back
of the queue and the step would then block on the whole backlog — which is how
a healthy cluster reported `did not finish within 900s (last state: queued)`.
A pending run picks up the Bronze just written anyway, since the Spark jobs read
every retained partition. If the queue is deep, raise `SILVER_DAG_TIMEOUT` or
clear old runs in the Airflow UI.

```text
  ✓  Seeded 200 orders into postgres-source
  ✓  Validation passed
  ✓  Uploaded 4 parquet file(s) to MinIO
  ✓  Loaded 200 rows into raw.orders_source via COPY (staging upsert)
  ✓  Transactions applied to source
  ✓  Row count matches
  ✓  All 5 deleted orders are gone from the mirror
  ✓  All 10 cancellations reflected in the mirror
  ✓  spark_transform_silver completed
  ✓  Iceberg tables present
  ✓  Lakehouse is readable and populated
  ✓  dbt marts present
  ✓  dbt marts populated

  Total: 13 passed, 0 failed, 0 skipped

  All checks passed — all three pipelines verified end to end:
    Pipe 1  warehouse: dbt int/gold marts built and populated
    Pipe 2  lakehouse: iceberg.lake.orders readable through Trino
    Pipe 3  mirror:    counts, deletes and cancellations in ClickHouse
```

A **skipped** check is not a passing one. If Trino is unreachable, or Airflow
cannot be reached to trigger the silver DAG, those steps report `~` and the
summary says the pipeline behind them is UNVERIFIED rather than counting them
green — which is what it used to do, for as long as it never read the lakehouse
at all (#149).

The mart check reads what the ingest DAG produced, so it still needs one
successful run of `ingest_source_to_bronze` behind it. On a cluster deployed
minutes ago it fails honestly rather than passing vacuously.

### Step 6 — Open the dashboards

Get your node IP:

```bash
kubectl get nodes -o wide   # EXTERNAL-IP column; use INTERNAL-IP if blank
```

| Dashboard | URL | Credentials |
|---|---|---|
| Airflow (pipeline runs) | `http://NODE_IP:30880` | admin / admin |
| AI Dashboard (ask questions in English) | `http://NODE_IP:30333` | `DASHBOARD_AUTH_*` from secrets |
| Kafka UI (topic monitoring) | `http://NODE_IP:30801` | `KAFKA_UI_*` from secrets |
| Grafana (metrics) | `http://NODE_IP:30300` | `AIRFLOW_ADMIN_*` from secrets — see note |
| MinIO (data files) | `http://NODE_IP:30901` | `MINIO_ROOT_*` from secrets |
| Spark UI (job progress) | `http://NODE_IP:30808` | — |

Credentials come from `k8s/01-secrets.generated.yaml` if you ran Step 2, otherwise from the defaults in `k8s/01-secrets.yaml`. Read any of them with:

```bash
kubectl get secret etl-secrets -n etl -o jsonpath='{.data.MINIO_ROOT_PASSWORD}' | base64 -d
```

> **Grafana uses the `AIRFLOW_ADMIN_*` keys, not a Grafana-specific pair.** The deployment maps `AIRFLOW_ADMIN_USER`/`AIRFLOW_ADMIN_PASSWORD` onto `GF_SECURITY_ADMIN_USER`/`GF_SECURITY_ADMIN_PASSWORD`, so the two services share one admin credential. It is not `admin/admin123`, and it is not the `DASHBOARD_AUTH_*` pair — those belong to the AI dashboard:
>
> ```bash
> kubectl get secret etl-secrets -n etl -o jsonpath='{.data.AIRFLOW_ADMIN_USER}' | base64 -d; echo
> kubectl get secret etl-secrets -n etl -o jsonpath='{.data.AIRFLOW_ADMIN_PASSWORD}' | base64 -d; echo
> ```

**Trino** (no NodePort — port-forward to query the lakehouse):

```bash
kubectl port-forward svc/trino 8080:8080 -n etl
# then http://localhost:8080 (any username), e.g.:
#   SELECT status, count(*) FROM iceberg.lake.orders GROUP BY status;
```

**ClickHouse** (query the real-time mirror):

```bash
kubectl exec -n etl clickhouse-0 -- clickhouse-client --user <CLICKHOUSE_USER> --password <CLICKHOUSE_PASSWORD> \
  -q "SELECT status, count() FROM mirror.orders_current GROUP BY status"
```

### Step 7 — Grafana dashboards

A **Data Platform Health** dashboard (pipeline runs, CDC lag, source freshness, revenue anomaly) is provisioned automatically. For deeper infrastructure views, import these community dashboards (**Dashboards → Import**, enter the ID, pick the Prometheus datasource):

| ID | Dashboard |
|---|---|
| 1860 | Node Exporter Full (CPU, memory, disk, network) |
| 9628 | PostgreSQL Databases (works with postgres_exporter; 9948 needs TimescaleDB — don't use it) |
| 315 | Kubernetes cluster monitoring |
| 13502 | MinIO |

To deliver alerts (DAG failures, CDC lag, stale data, revenue anomalies), add a Slack/email receiver in the `alertmanager-config` ConfigMap.

### Step 8 — Watch live data flow through all three pipelines

Step 5 proves the pipelines work once. To watch a continuous stream of new records land in all three destinations, generate live traffic against the source.

**Generate traffic.** Port-forward the source and run the generator — it only ever appends and updates, so CDC, the mirror and the batch high-water mark all stay valid while it runs:

```bash
kubectl port-forward svc/postgres-source 5432:5432 -n etl
# in another terminal, from the repo root:
python3 -m pip install psycopg2-binary
python3 scripts/simulate_live_traffic.py --rate 5 --interval 3
```

**Watch the mirror keep up.** In a third terminal, poll the source and the mirror together. Any gap between the two columns is the live end-to-end CDC lag:

```bash
CH_USER=$(kubectl get secret etl-secrets -n etl -o jsonpath='{.data.CLICKHOUSE_USER}' | base64 -d)
CH_PASS=$(kubectl get secret etl-secrets -n etl -o jsonpath='{.data.CLICKHOUSE_PASSWORD}' | base64 -d)

for _ in $(seq 1 20); do
  src=$(kubectl exec -n etl postgres-source-0 -- \
          bash -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT count(*) FROM orders"')
  mir=$(kubectl exec -n etl clickhouse-0 -- clickhouse-client \
          --user "$CH_USER" --password "$CH_PASS" \
          -q "SELECT count() FROM mirror.orders_current")
  echo "$(date +%T)  source: ${src// /}  mirror: ${mir}  behind: $(( ${src// /} - mir ))"
  sleep 5
done
```

Expect the mirror to trail by a few seconds and never fall further behind. Steady lag means the consumer is keeping up; lag that grows run after run is the signal something is wrong. The gap itself is mostly ClickHouse's `stream_flush_interval_ms` (7.5s by default), so a few seconds is normal and not a sign of trouble.

Leave it running and watch each destination:

| Pipeline | Where to look | When it moves |
|---|---|---|
| **3 · Real-time mirror** | `mirror.orders_current` in ClickHouse — the loop above, or rerun the count by hand | seconds |
| **1 · Warehouse** | `raw.orders_source`, then `int`/`gold` after dbt runs | on the hourly run |
| **2 · Lakehouse** | `iceberg.lake.orders` via Trino | on the hourly run, after the silver DAG follows ingestion |

The source count and the ClickHouse count should track each other continuously. The warehouse and lakehouse catch up in steps, one batch per run — that difference **is** the architecture, and watching the three side by side is the clearest demonstration of it.

The batch DAGs run hourly, so leave the generator going and check back. To see a batch cycle immediately instead of waiting, trigger `ingest_source_to_bronze` from the Airflow UI (`http://NODE_IP:30880`) — it runs on demand and the silver DAG follows it.

> Do not use `make seed` / `generate_ecommerce.py` for this: that script drops and recreates the source tables, which breaks the replication slot mid-test and leaves stale rows in the mirror. `simulate_live_traffic.py` is the non-destructive one.

---

## Troubleshooting

### docker push fails

```bash
docker login   # then retry
```

### kubectl cluster-info fails

```bash
export KUBECONFIG=/path/to/your/kubeconfig
kubectl cluster-info
```

### Pods stuck in Pending

Read the `Events` line of `kubectl describe pod <pod> -n etl` and note *which* reason it gives — they need opposite fixes and look identical at a glance.

`Insufficient cpu` or `Insufficient memory` is a capacity problem: something requested more than the node has left.

A **taint** is not. This is the one to recognise:

```text
0/2 nodes are available:
  1 node(s) had untolerated taint {node-role.kubernetes.io/control-plane: }
  1 node(s) had untolerated taint {node.kubernetes.io/disk-pressure: }
```

The control-plane taint is normal and permanent. `node.kubernetes.io/disk-pressure` is the kubelet reporting that the node's root filesystem has crossed its eviction threshold (10–15% free depending on distribution). It taints the node `NoSchedule` and starts evicting pods, so *nothing* new schedules regardless of what it requests. Adjusting resource requests will not help.

Check the node itself — and mind that these commands are node-local, so run them on the node that is actually complaining, not the control-plane:

```bash
kubectl describe node <node> | grep -A8 Conditions
ssh <node> df -h /
ssh <node> "sudo du -xh --max-depth=1 / 2>/dev/null | sort -h | tail -12"
```

The alerts in `NodeDiskFillingUp` / `NodeDiskNearEvictionThreshold` are meant to catch this days earlier. If they never fired, confirm node-exporter is reading the host filesystem rather than its own container — it needs `--path.rootfs=/host` and a read-only `/` mount, both of which the manifests set.

### PVC sizes are not limits on a local StorageClass

`local-path` (and `hostPath`) hand out a plain directory on the node. The `storage:` figure in a PVC is a label with no mechanism behind it: no quota, no device, no separate filesystem. A pod can write past its request until the **node's** disk is full, and every byte counts against the same filesystem the kubelet watches for eviction.

The default manifests claim roughly 300Gi in total. On a single 100G node every claim still binds and the cluster reports itself healthy — right up until one workload grows into the space and takes the node down with it. On one cluster ClickHouse reached 34G against a 20Gi claim and evicted the Airflow control plane.

If your nodes are smaller than the sum of the claims, either point the StorageClass at real volumes, or lower the requests **before first deploy**. Lowering them afterwards is not a live change: `volumeClaimTemplates` are immutable, so `kubectl apply` and `helm upgrade` both reject it. Changing one means deleting the StatefulSet with `--cascade=orphan`, editing, and re-applying — plan it, do not discover it mid-incident.

Watch the actual consumption rather than the claims:

```bash
ssh <node> "sudo du -xh --max-depth=2 /opt/local-path-provisioner | sort -h | tail"
```

### Pods Evicted, or the node reports DiskPressure

```bash
kubectl describe node | grep DiskPressure
```

If that says `True`, the node is out of disk and the kubelet is **rejecting** new pods — so you will see dozens of Evicted pods appear within seconds of each other. They were never running; they were turned away on arrival. Nothing schedules until space is freed.

The usual culprit is Docker's build store, not the pipeline. `deploy.sh` rebuilds a 5GB+ Airflow image on every run, and the previous layers plus the BuildKit cache stay behind. The script now prunes what it orphans, but a machine with a backlog from earlier deploys needs one manual pass:

```bash
docker system prune -a --volumes    # Docker's store: build layers and cache
sudo crictl rmi --prune             # containerd's store: images Kubernetes runs from
sudo journalctl --vacuum-size=500M  # systemd journal, often several GB
```

Those are two separate stores. `crictl` will not touch Docker's, and pruning Docker cannot disturb anything running on the cluster.

Find what is actually holding the space before guessing:

```bash
df -h /
sudo du -xh --max-depth=1 / | sort -h | tail -10
```

If that points at a PersistentVolume rather than a Docker or containerd store,
the workload inside it is the thing to look at. For ClickHouse — the largest
claim in this stack, and the one that took a node down in #144 — see
[Which ClickHouse table is using the space](#which-clickhouse-table-is-using-the-space).

Once `DiskPressure` returns to `False`, the rejected pods schedule on their own. This just tidies the list:

```bash
kubectl delete pod -n etl --field-selector status.phase=Failed
```

### A config file change did not take effect

Editing a file under `docker/clickhouse/config.d/` and restarting the pod does nothing on Kubernetes. Under Compose the file is bind-mounted, so an edit plus a restart is genuinely the whole story — that difference is what makes this easy to get wrong.

On Kubernetes the file reaches the container through three links, and every one of them has to be updated:

1. **The ConfigMap.** `deploy.sh` builds `clickhouse-config` from the whole directory *at deploy time* (`k8s/deploy.sh:256`). Your checkout is not mounted into the cluster; a `git pull` alone changes nothing the pod can see.
2. **The mount.** Each file gets its own `subPath` entry in `k8s/clickhouse/statefulset.yaml`. Mounting the directory whole would replace the image's `docker_related_config.xml`, which sets `listen_host` — without it the server binds loopback only and every probe gets connection refused. So **a new file needs a new mount**, and a mount only exists in the cluster once the manifest has been applied.
3. **The pod.** `config.d` is read at startup, so the pod must restart.

`kubectl rollout restart` on its own only does the third. It restarts pods against the StatefulSet spec **already in the cluster**, so a mount added in the repo but never applied does not exist as far as the restart is concerned.

Re-running the deploy does all three:

```bash
SEED=n bash k8s/deploy.sh
```

Verify by looking in the container rather than trusting the output:

```bash
kubectl exec -n etl clickhouse-0 -- ls /etc/clickhouse-server/config.d/
```

Every file you expect should be listed. If one is missing, its `subPath` mount is missing — no amount of restarting will change that.

Do **not** apply the raw manifest by hand:

```bash
kubectl apply -f k8s/clickhouse/statefulset.yaml   # will be rejected
```

The file carries `storageClassName: standard` and `deploy.sh` rewrites that to your actual StorageClass (`k8s/deploy.sh:128`). Since `volumeClaimTemplates` is immutable, the apply fails with a `Forbidden: updates to statefulset spec for fields other than...` error that names the storage class rather than whatever you were trying to change. To skip a full deploy, patch the pod template only:

```bash
kubectl patch statefulset clickhouse -n etl --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/volumeMounts/-","value":{"name":"server-config","mountPath":"/etc/clickhouse-server/config.d/<file>.xml","subPath":"<file>.xml"}}]'
```

### Which ClickHouse table is using the space

The node-level `du` above tells you the ClickHouse volume is large. It cannot
tell you which table filled it, and the answer is rarely the one people expect.
Ask the server:

```sql
SELECT database, table,
       formatReadableSize(sum(bytes_on_disk)) AS size,
       sum(rows) AS rows, count() AS parts,
       min(min_date) AS oldest, max(max_date) AS newest
FROM system.parts
WHERE active
GROUP BY database, table
ORDER BY sum(bytes_on_disk) DESC
LIMIT 20;
```

Expect `system` to dominate and `mirror` to be small. On one cluster the four
mirror tables came to under 600 KiB between them while `system.trace_log` alone
held 750 MiB. That is the normal shape rather than a fault: the mirror stores
one row per source row, and the profiler stores a sampled stack trace per
running query per second — and every Kafka Engine table polls without pause, so
the server always looks busy to it.

`oldest` and `newest` read `1970-01-01` for the `mirror` tables. Those two
columns are only populated for a `Date`-derived partition key, and the mirror
partitions on a `DateTime64` (`PARTITION BY toYYYYMM(created_at)`), so they stay
at the epoch. It says nothing about the data — read `system.parts.partition`
instead.

To look at the mirror on its own:

```sql
SELECT table, formatReadableSize(sum(bytes_on_disk)) AS size,
       sum(rows) AS rows, count() AS parts
FROM system.parts WHERE database = 'mirror' AND active
GROUP BY table ORDER BY sum(bytes_on_disk) DESC;
```

If a large table under `system` has a name ending in a number, the retention
config has been applied and the old unbounded copy is still on disk — which is
the next section.

### ClickHouse system tables keep their old settings after a config change

`02_system_log_retention.xml` sets a partition key and TTL on ClickHouse's internal log tables. ClickHouse will not rewrite an existing table to match: at startup it renames the old one aside — `text_log` becomes `text_log_0`, then `text_log_1` — and creates a fresh empty table with the new definition.

New writes are bounded immediately, but **the old data stays on disk under the renamed table** and no TTL applies to it. Applying the retention config therefore reclaims nothing by itself. List them and drop them:

```sql
SHOW TABLES FROM system LIKE '%log%';
DROP TABLE system.text_log_0;   -- repeat for each numeric suffix
```

The same renaming happens on a ClickHouse version upgrade, so a long-lived cluster accumulates these whether or not anyone edits the config.

### A DAG run stays queued and its tasks show "No Status"

The DAG is paused. A paused DAG still accepts a trigger — the run is created,
stays `queued`, and every task instance sits at `No Status` with try number 0.
Nothing fails, so there is no error to find, and it reads as a broken scheduler.

```bash
kubectl exec -n etl deploy/airflow-scheduler -- airflow dags list
```

The `is_paused` column is the answer. To start them:

```bash
kubectl exec -n etl deploy/airflow-scheduler -- airflow dags unpause ingest_source_to_bronze
kubectl exec -n etl deploy/airflow-scheduler -- airflow dags unpause spark_transform_silver
```

Deployments from this repo set `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False`,
so newly registered DAGs start unpaused. A cluster deployed before that, or a DAG
paused by hand, still needs the commands above.

### A test step fails

```bash
kubectl get pods -n etl           # find the pod name
kubectl logs -n etl <pod-name>    # read the logs
```

### Upgrading from an older deployment

`deploy.sh` handles most migrations automatically: it removes the retired cdc-sync-daemon and the old raw-manifest Trino before installing the Helm release. Nothing to run by hand for those.

One migration it cannot do for you: Kafka storage moved from ephemeral to persistent volumes. Strimzi cannot change the storage type of a running cluster, so if your cluster predates this, delete the Kafka CR once before redeploying (in-flight messages are lost — they were on ephemeral storage anyway, and the CDC connector re-syncs):

```bash
kubectl delete kafka etl-kafka -n etl --ignore-not-found
bash k8s/deploy.sh   # recreates Kafka on persistent volumes
```

---

## Quick Reference

```text
INSTALL   kubectl + helm + docker + docker login
CLONE     git clone https://github.com/kalluripradeep/modern-etl-stack.git
SECRETS   bash k8s/generate-secrets.sh
DEPLOY    bash k8s/deploy.sh
WAIT      kubectl get pods -n etl -w
TEST      bash scripts/test_e2e.sh
OPEN      Airflow :30880 · AI Dashboard :30333 · Grafana :30300 · MinIO :30901 · Spark :30808 · Kafka UI :30801
TRINO     kubectl port-forward svc/trino 8080:8080 -n etl
GRAFANA   Data Platform Health is pre-loaded; import 1860 / 9628 / 315 / 13502 for infra views
```
