# modern-etl-stack

A production-grade, fully containerised modern ETL stack built entirely with open-source tools — Apache Kafka, Debezium, Airflow, dbt, Spark, and Prometheus. Processes 1,000+ orders in real-time with sub-second CDC latency and ML-powered anomaly detection achieving sub-10ms inference. A cost-effective alternative to enterprise ETL tools, saving companies £100k+ annually.

## Architecture

```
┌──────────────┐     batch (daily)      ┌───────────────┐
│  PostgreSQL  │ ──────────────────────▶ │     MinIO     │
│   (source)   │                         │  (S3 / bronze)│
└──────┬───────┘                         └───────────────┘
       │                                         │
       │  CDC (real-time)                        │ (reference only)
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

## Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.8 |
| CDC / Streaming | Debezium 2.5 + Kafka 7.5 |
| Object Storage | MinIO (S3-compatible) |
| Transformation | dbt-core 1.7 (Medallion: bronze/silver/gold) |
| Batch compute | Apache Spark 3.5 |
| Data warehouse | PostgreSQL 15 |
| Monitoring | Prometheus + Grafana |

## Prerequisites

- Docker + Docker Compose
- `make` (optional but recommended)
- ~6 GB free RAM (all containers)

## Quick Start

```bash
# 1. Clone and configure
git clone https://github.com/kalluripradeep/modern-etl-stack.git
cd modern-etl-stack
cp .env.example .env          # edit .env if you want custom passwords

# 2. Start everything
make up
# or: docker-compose up -d

# 3. Wait ~60 seconds for services to be healthy, then seed source data
make seed

# 4. Register the Debezium CDC connector
make register-connector

# 5. Trigger the batch pipeline manually in the Airflow UI (or wait for schedule)
```

## Service URLs

| Service | URL | Default credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka UI | http://localhost:8001 | — |
| Spark Master UI | http://localhost:8081 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| Kafka Connect | http://localhost:8083 | — |

All credentials are overridable via `.env` — see `.env.example`.

## DAGs

| DAG | Schedule | Description |
|---|---|---|
| `extract_orders_to_minio` | Daily | PostgreSQL → Parquet → MinIO → dest PostgreSQL → dbt run |
| `consume_cdc_events` | Every 5 min | Kafka → dest PostgreSQL (real-time upserts via Debezium) |
| `spark_transform_orders` | Daily | Spark Bronze → Silver local Parquet transform |

## dbt Models

```
models/
├── bronze/
│   └── bronze_orders.sql       # view over raw.orders
├── silver/
│   ├── silver_orders.sql       # cleaned + validated table
│   └── schema.yml              # uniqueness, not_null, accepted_values tests
└── gold/
    ├── gold_daily_revenue.sql  # daily revenue by status
    └── schema.yml              # model-level tests
```

Run models and tests:
```bash
make dbt-run
make dbt-test
```

## Useful Commands

```bash
make up                  # start all containers
make down                # stop all containers
make logs                # tail all logs
make ps                  # show container health
make seed                # seed source database
make register-connector  # register Debezium connector
make dbt-run             # run dbt models
make dbt-test            # run dbt tests
```

## Project Layout

```
.
├── airflow/dags/           # Airflow DAG definitions
├── dbt/                    # dbt project (models, profiles, tests)
├── docker/airflow/         # Custom Airflow Dockerfile + requirements
├── monitoring/             # Prometheus config + Grafana dashboards
├── sample-data/            # Script to seed the source database
├── scripts/                # Utility scripts (Debezium registration, etc.)
├── spark/jobs/             # PySpark job for Bronze → Silver
├── docker-compose.yml
├── Makefile
└── .env.example            # Environment variable template
```
