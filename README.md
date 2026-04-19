# Modern ETL Infrastructure: Enterprise Data Lakehouse

A comprehensive ETL stack demonstrating the integration of open-source data engineering tools. This project features a **Dual-Engine Architecture**: running a low-latency relational Data Warehouse (dbt + PostgreSQL) in parallel with an unlimited-scale Data Lakehouse (Apache Spark + Apache Iceberg) powered by Airflow Change Data Capture (CDC).

## Architecture

```text
                                  ┌───────────────┐
                                  │  PostgreSQL   │
                                  │   (source)    │
                                  └──────┬────────┘
                                         │  1. High-Water Mark CDC 
                                         │  (Extracts Delta Only)
                                         ▼
                                  ┌───────────────┐
                                  │  Airflow DAG  │
                                  │  (Ingestion)  │
                                  └──────┬────────┘
                                         │
             ┌───────────────────────────┴────────────────────────────┐
             │                           │                            │
             ▼                           ▼                            ▼
      2A. UPSERT Delta            2B. Write Parquet           3B. Iceberg Delta 
   ┌─────────────────┐           ┌───────────────┐           ┌─────────────────┐
   │   PostgreSQL    │           │     MinIO     │──────────▶│  Apache Spark   │
   │  (destination)  │           │   (Bronze S3) │           │ (Silver/Iceberg)│
   └────────┬────────┘           └───────────────┘           └─────────┬───────┘
            │                                                          │
            ▼                                                          ▼
   ┌─────────────────┐                                       ┌─────────────────┐
   │    dbt Core     │  ◀───── 3A. Incremental Merges        │ Iceberg Catalog │
   │ (Silver/Gold)   │                                       │ (Compaction /   │
   └─────────────────┘                                       │  Z-Ordering)    │
                                                             └─────────────────┘

Monitoring: Prometheus + Grafana + Node Exporter
```

## Technology Stack

| Layer | Component |
|---|---|
| **Orchestration** | Apache Airflow 2.8 & Cosmos |
| **Object Storage** | MinIO (S3-compatible) |
| **Transformation** | dbt-core 1.7 (Incremental Models) |
| **Batch Compute** | Apache Spark 3.5 |
| **Lakehouse Format**| Apache Iceberg 1.4 |
| **Data Warehouse** | PostgreSQL 15 |
| **Monitoring** | Prometheus & Grafana |

## Core Data Pipelines

### 1. The High-Water Mark CDC (`ingest_source_to_bronze.py`)
This is the master ingestion DAG. It queries the destination database for the `MAX(updated_at)` timestamp and seamlessly injects it into the extraction query against the source database. This guarantees that only **modified or new rows** (the delta) are extracted, entirely bypassing expensive full-table reloads.

It handles data dispatching by:
1. Staging the delta into a PostgreSQL temporary table and executing `ON CONFLICT DO UPDATE`.
2. Saving the delta as a historical Parquet blob inside MinIO's `s3a://bronze` layer.
3. Dynamically triggering the downstream Spark Data Lakehouse and dbt processing jobs.

### 2. The Analytical Warehouse (`dbt`)
Configured to use `materialized='incremental'` across the Silver layer, the dbt pipeline runs within Airflow (via Astronomer Cosmos). It natively detects new incremental batches and elegantly merges them to uphold sub-second BI dashboard latency for current data.

### 3. The Scalable Data Lakehouse (`spark_transform_silver.py`)
Triggered automatically by Airflow, four heavy PySpark jobs natively download the parquet deltas from MinIO and execute `MERGE INTO` SQL commands against **Apache Iceberg**. This allows unlimited structural scaling without database locking constraints.
* A weekly `iceberg_maintenance.py` job sequentially fires to execute small-file `rewrite_data_files` (Compaction) and apply advanced **Z-Ordering algorithms** on core partition clusters.

## Quick Start

```bash
# 1. Clone repository and initialize environment variables
cp .env.example .env 

# 2. Spin up containers
make up

# 3. Wait ~60 seconds for services to reach healthy state, then seed database
make seed
```

## Service Access URLs

| Service | Local URL | Credentials (Default) |
|---|---|---|
| **Airflow UI** | http://localhost:8080 | admin / admin |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |
| **Spark Master UI** | http://localhost:8081 | — |
| **Grafana** | http://localhost:3000 | admin / admin |
| **Prometheus** | http://localhost:9090 | — |

## Project Operations

```bash
make up                  # Start infrastructure
make down                # Tear down infrastructure
make logs                # Tail aggregated container logs
make ps                  # Service health check
make seed                # Generate sample eCommerce data
make dbt-run             # Execute manual dbt transformation
make dbt-test            # Execute dbt data quality tests
```
