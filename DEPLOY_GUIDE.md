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
| Airflow (pipeline runs) | `http://NODE_IP:30880` | admin | admin123 |
| Grafana (metrics) | `http://NODE_IP:30300` | admin | admin123 |
| MinIO (data files) | `http://NODE_IP:30901` | minioadmin | minioadmin123 |
| Spark UI (job progress) | `http://NODE_IP:30808` | — | — |

Replace `NODE_IP` with the IP address you got from the command above.

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
OPEN      Airflow → :30880  Grafana → :30300  MinIO → :30901  Spark → :30808
```
