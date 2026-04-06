# Modern ETL Infrastructure

A comprehensive ETL stack demonstrating the integration of open-source data engineering tools. This project features real-time CDC (Change Data Capture) and batch processing pipelines orchestrating data movement between a staging relational database, a data lake, and a destination database. 

## Architecture

```text
┌──────────────┐     batch (daily)      ┌───────────────┐
│  PostgreSQL  │ ──────────────────────▶ │     MinIO     │
│   (source)   │                         │  (S3 / bronze)│
└──────┬───────┘                         └───────────────┘
       │                                         │
       │  CDC (real-time)                        │
       ▼                                         ▼
┌──────────────┐    Kafka topic     ┌───────────────────┐
│   Debezium   │ ──────────────────▶│  Airflow CDC DAG  │
│ (Kafka Conn.)│                    └────────┬──────────┘
└──────────────┘                             │ upserts
                                             ▼
                                    ┌──────────────────┐
                                    │   PostgreSQL     │
                                    │  (destination)   │
                                    └────────┬─────────┘
                                             │
                                    ┌────────▼─────────┐
                                    │       dbt        │
                                    │ bronze→silver→gold│
                                    └──────────────────┘
                                             │
                                    ┌────────▼──────────┐
                                    │  Spark (optional) │
                                    │ Bronze→Silver job │
                                    └───────────────────┘

Monitoring: Prometheus + Grafana + Node Exporter
```

## Technology Stack

| Layer | Component |
|---|---|
| **Orchestration** | Apache Airflow 2.8 |
| **CDC / Streaming** | Debezium 2.5 & Confluent Kafka 7.5 |
| **Object Storage** | MinIO (S3-compatible) |
| **Transformation** | dbt-core 1.7 |
| **Batch Compute** | Apache Spark 3.5 |
| **Data Warehouse** | PostgreSQL 15 |
| **Monitoring** | Prometheus & Grafana |

## Prerequisites

- Docker + Docker Compose
- `make` (optional but recommended)
- ~6 GB free RAM available for Docker daemon

### Linux Permissions Note

If you encounter `permission denied` errors when running Airflow containers, you may need to adjust local directory ownership to match the `airflow` user's UID (50000):
```bash
sudo chown -R 50000:0 logs dags plugins
sudo chmod -R 775 logs dags plugins
```

## Quick Start

```bash
# 1. Clone repository and initialize environment variables
cp .env.example .env 

# 2. Spin up containers
make up

# 3. Wait ~60 seconds for services to reach healthy state, then seed database
make seed

# 4. Register the Debezium CDC connector
make register-connector
```

## Service Access URLs

| Service | Local URL | Credentials (Default) |
|---|---|---|
| **Airflow UI** | http://localhost:8080 | admin / admin |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |
| **Kafka UI** | http://localhost:8001 | — |
| **Spark Master UI** | http://localhost:8081 | — |
| **Grafana** | http://localhost:3000 | admin / admin |
| **Prometheus** | http://localhost:9090 | — |
| **Kafka Connect** | http://localhost:8083 | — |

## Data Pipelines

### Airflow DAGs

1. **`extract_orders_to_minio`** *(Daily)*
   - Batch extraction of PostgreSQL source.
   - Converts to Parquet, validates data quality, and uploads to MinIO.
   - Replicates to Destination PostgreSQL database for downstream modeling.
2. **`consume_cdc_events`** *(Every 5 Minutes)*
   - Micro-batch consumer. 
   - Reads serialized events from Kafka (Debezium source) and executes Upserts/Deletes on the target schema.
3. **`spark_transform_orders`** *(Daily)*
   - Submits PySpark applications for heavy compute processing (Bronze -> Silver translation) over MinIO.

### dbt Modeling

```text
dbt/models/
├── bronze/
│   └── bronze_orders.sql
├── silver/
│   ├── silver_orders.sql
│   └── schema.yml
└── gold/
    ├── gold_daily_revenue.sql
    └── schema.yml
```

Execute models and test suites:
```bash
make dbt-run
make dbt-test
```

## Project Operations

```bash
make up                  # Start infrastructure
make down                # Tear down infrastructure
make logs                # Tail aggregated container logs
make ps                  # Service health check
make seed                # Generate sample source data
make register-connector  # Initialize Debezium CDC connector
make dbt-run             # Execute dbt transformation
make dbt-test            # Execute dbt validation tests
```
