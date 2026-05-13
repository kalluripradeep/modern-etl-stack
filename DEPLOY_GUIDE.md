# ETL Stack — Deployment Guide

This guide walks you through deploying the full ETL stack on your Kubernetes cluster.

---

## What you are deploying

A production-grade ETL pipeline that handles 500M+ records with:

- **PostgreSQL** — source and destination databases
- **Kafka + Debezium** — real-time change data capture (CDC)
- **MinIO** — S3-compatible object storage (bronze + silver layers)
- **Apache Spark** — large-scale data transformation
- **Apache Airflow** — pipeline orchestration
- **Prometheus + Grafana** — monitoring and dashboards

---

## Prerequisites

Install these three tools on your machine before starting.

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

Verify:
```bash
kubectl version --client
```

### 2. Helm
```bash
# Mac
brew install helm

# Windows
choco install kubernetes-helm

# Linux
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

Verify:
```bash
helm version
```

### 3. Docker
Download and install from: **https://docs.docker.com/get-docker/**

Verify:
```bash
docker --version
```

### 4. Docker Hub account (free)
You need this to push the custom Airflow image.
Sign up at: **https://hub.docker.com**

After signing up, log in on your machine:
```bash
docker login
# enter your Docker Hub username and password
```

### 5. Kubernetes cluster access
Make sure kubectl is pointing at your cluster:
```bash
kubectl cluster-info
```
You should see your cluster URL, not an error.
If this fails, contact whoever gave you cluster access and ask for the kubeconfig file.

---

## Deployment Steps

### Step 1 — Clone the repository
```bash
git clone https://github.com/kalluripradeep/modern-etl-stack.git
cd modern-etl-stack
```

### Step 2 — Configure Storage Class
Before deploying, you must ensure your stateful pods know what hard drive type to request, or else your pods will be stuck in a `Pending` state.

Find your cluster's storage class:
```bash
kubectl get storageclass
```

Then open these files and replace `standard` with the name you found:
- `k8s/postgres-source/statefulset.yaml`
- `k8s/postgres-dest/statefulset.yaml`
- `k8s/kafka/statefulset.yaml`
- `k8s/zookeeper/statefulset.yaml`
- `k8s/minio/statefulset.yaml`

*Common values:*
- AWS → `gp3`
- GCP → `standard` or `pd-ssd`
- Azure → `default`
- Minikube/Kind → `standard`

### Step 3 — Run the deploy script
```bash
bash k8s/deploy.sh
```

The script will ask you one question:
```
Enter your registry (e.g. docker.io/myuser):
```
Type your Docker Hub username like this and press Enter:
```
docker.io/yourname
```

The script will then automatically:
- Build and push the Airflow Docker image
- Create the Kubernetes namespace
- Deploy all databases, Kafka, MinIO, Spark, Airflow, and monitoring
- Create the MinIO bronze and silver buckets
- Register the Debezium CDC connector

This takes about **5-10 minutes** on first run.

### Step 4 — Wait for all pods to be Running
```bash
kubectl get pods -n etl -w
```

Wait until every pod shows `Running`. Press `Ctrl+C` when done.

It should look like this:
```
NAME                            READY   STATUS
kafka-0                         1/1     Running
kafka-connect-xxx               1/1     Running
minio-0                         1/1     Running
postgres-dest-0                 1/1     Running
postgres-source-0               1/1     Running
spark-master-xxx                1/1     Running
spark-worker-xxx                1/1     Running
zookeeper-0                     1/1     Running
airflow-webserver-xxx           1/1     Running
airflow-scheduler-xxx           1/1     Running
prometheus-xxx                  1/1     Running
grafana-xxx                     1/1     Running
```

### Step 5 — Run the test with real transactional data
```bash
bash scripts/test_e2e.sh
```

This automatically:
- Seeds 200 real orders into the source database
- Runs the full ETL pipeline (extract → MinIO → load to warehouse)
- Fires real transactions (status updates, cancellations, hard deletes)
- Verifies CDC captured every change correctly
- Prints a pass/fail report

Expected output at the end:
```
  ✓  Seeded 200 orders into postgres-source
  ✓  Validation passed
  ✓  Uploaded parquet files to MinIO
  ✓  Loaded rows into postgres-dest
  ✓  Transactions applied (40 updates, 10 cancellations, 5 deletes)
  ✓  CDC events consumed
  ✓  Row count matches
  ✓  All deleted orders are gone from dest
  ✓  All cancellations reflected in dest

  Total: 10 passed, 0 failed
  All checks passed — pipeline is healthy!
```

### Step 6 — Open the dashboards

First get your node IP:
```bash
kubectl get nodes -o wide
# look at the EXTERNAL-IP column
# if EXTERNAL-IP is blank, use the INTERNAL-IP
```

Then open these in your browser:

| Dashboard | URL | Username | Password |
|---|---|---|---|
| Airflow (pipeline runs) | `http://NODE_IP:30880` | admin | admin |
| Kafka UI (topic monitoring) | `http://NODE_IP:30801` | — | — |
| Grafana (metrics) | `http://NODE_IP:30300` | admin | admin123 |
| MinIO (data files) | `http://NODE_IP:30901` | minioadmin | minioadmin123 |
| Spark UI (job progress) | `http://NODE_IP:30808` | — | — |

Replace `NODE_IP` with the IP address you got from the command above.

### Step 7 — Load the Node Exporter Full Dashboard in Grafana

The deploy script provisions a placeholder dashboard automatically. To load the **full** Node Exporter dashboard (30+ panels for CPU, memory, disk, and network):

1. Open Grafana at `http://NODE_IP:30300` and log in.
2. Click **Dashboards → Import** in the left sidebar.
3. In the **"Import via grafana.com"** field, enter:
   ```
   1860
   ```
4. Click **Load**.
5. Under **"Prometheus"**, select **Prometheus** from the dropdown.
6. Click **Import**.

You should now see the full **Node Exporter Full** dashboard with live metrics from your cluster.

> **Reference:** [grafana.com/grafana/dashboards/1860](https://grafana.com/grafana/dashboards/1860-node-exporter-full/)

### Step 8 — Load the PostgreSQL Dashboard in Grafana

The deploy script automatically starts a `postgres_exporter` sidecar for both the source and destination databases. This feeds `pg_stat_*` metrics into Prometheus so you can visualise query performance, connections, cache hit rates, and more.

> **Why not dashboard 9948?** Dashboard 9948 (*PostgreSQL Infrastructure*) requires **TimescaleDB** and **collectd**, which are not installed in this stack. Use **dashboard 9628** instead — it works with standard `postgres_exporter`.

To load the dashboard:

1. Open Grafana at `http://NODE_IP:30300` and log in.
2. Click **Dashboards → Import** in the left sidebar.
3. In the **"Import via grafana.com"** field, enter:
   ```
   9628
   ```
4. Click **Load**.
5. Under **"Prometheus"**, select **Prometheus** from the dropdown.
6. Click **Import**.

You should now see live PostgreSQL metrics for both `postgres-source` and `postgres-dest`.

> **Reference:** [grafana.com/grafana/dashboards/9628](https://grafana.com/grafana/dashboards/9628-postgresql-databases/)

### Step 9 — Load the Kubernetes Cluster Dashboard in Grafana

Prometheus is configured to scrape Kubernetes metrics (cAdvisor, kubelet, and nodes) directly from the cluster. You can use these metrics to monitor Pod and Node resource usage across the entire namespace.

1. Open Grafana at `http://NODE_IP:30300` and log in.
2. Click **Dashboards → Import** in the left sidebar.
3. In the **"Import via grafana.com"** field, enter:
   ```
   315
   ```
4. Click **Load**.
5. Under **"Prometheus"**, select **Prometheus** from the dropdown.
6. Click **Import**.

You should now see the **Kubernetes cluster monitoring (via Prometheus)** dashboard displaying CPU/Memory utilization for the ETL pods.

> **Reference:** [grafana.com/grafana/dashboards/315](https://grafana.com/grafana/dashboards/315-kubernetes-cluster-monitoring-via-prometheus/)

### Step 10 — Load the MinIO Dashboard in Grafana

The deploy script configures MinIO to expose Prometheus metrics (v2 API) on port 9000, and wires Prometheus to scrape them.

1. Open Grafana at `http://NODE_IP:30300` and log in.
2. Click **Dashboards → Import** in the left sidebar.
3. In the **"Import via grafana.com"** field, enter:
   ```
   13502
   ```
4. Click **Load**.
5. Under **"Prometheus"**, select **Prometheus** from the dropdown.
6. Click **Import**.

You will now see full observability into the Data Lake storage usage (buckets, object counts, traffic, and disk performance).

> **Reference:** [grafana.com/grafana/dashboards/13502](https://grafana.com/grafana/dashboards/13502-minio-dashboard/)

---

## Troubleshooting

### docker push fails
```bash
docker login
# enter your Docker Hub username and password, then retry
```

### kubectl cluster-info fails
Your kubeconfig is not configured. Ask whoever manages your cluster for the kubeconfig file and run:
```bash
export KUBECONFIG=/path/to/your/kubeconfig
kubectl cluster-info   # try again
```

### A test step fails
Check the logs of the failing service:
```bash
kubectl get pods -n etl           # find the pod name
kubectl logs -n etl <pod-name>    # read the logs
```
Share the output for help debugging.

---

## Quick Reference

```
INSTALL   kubectl + helm + docker + docker login
CLONE     git clone https://github.com/kalluripradeep/modern-etl-stack.git
DEPLOY    bash k8s/deploy.sh
WAIT      kubectl get pods -n etl -w
TEST      bash scripts/test_e2e.sh
OPEN      Airflow → :30880  Grafana → :30300  MinIO → :30901  Spark → :30808  Kafka UI → :30801
GRAFANA   Import ID 1860 (Node) + 9628 (Postgres) + 315 (K8s) + 13502 (MinIO)
```
