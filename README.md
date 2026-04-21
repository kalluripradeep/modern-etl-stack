# Modern ETL Infrastructure

A comprehensive ETL stack demonstrating the integration of open-source data engineering tools. This project features both **real-time CDC (Change Data Capture)** via Kafka/Debezium and **high-scale batch processing** pipelines orchestrating data movement between a staging relational database, a data lake, and a destination database. 

## Architecture

The system utilizes a **Definitive "Three-Track" Architecture**. This design separates highspeed operational mirrors, business-critical analytics, and massive historical archives into independent, parallel tracks.

```text
           ┌──────────────┐
           │  PostgreSQL  │  (Source Transactional Database)
           │   (source)   │
           └──────┬───────┘
                  │
      ┌───────────┼───────────┐
      ▼           ▼           ▼
  TRACK 1:      TRACK 2:    TRACK 3:
 OPERATIONAL   ANALYTICAL   BIG DATA
  (Mirror)    (Warehouse)  (Lakehouse)
      │           │           │
┌─────▼─────┐     │     ┌─────▼─────┐
│ Debezium  │     │     │  Airflow  │ (The Courier)
└─────┬─────┘     │     └─────┬─────┘
      │           │           │
┌─────▼─────┐     │     ┌─────▼─────┐
│   Kafka   │     │     │  MinIO    │ (Bronze / Raw)
└─────┬─────┘     │     └─────┬─────┘
      │      ┌────▼────┐      │
┌─────▼─────┐│ Airflow │┌─────▼─────┐
│ JDBC Sink │└────┬────┘│  Spark    │ (The Chef)
└─────┬─────┘     │     └─────┬─────┘
      │           │           │
┌─────▼─────┐     │     ┌─────▼─────┐
│  public   │ ◀───┘     │  Silver   │ (Iceberg - 1Bn+ Rows)
│  (raw)    │           │ (Lakehouse)│
└─────┬─────┘           └───────────┘
      │                      
┌─────▼─────┐          
│    dbt    │ (The Brain)
└─────┬─────┘          
      ▼                
┌────────────┐         
│ analytics  │         
│   (Gold)   │         
└────────────┘         
  PostgreSQL (Destination)
```

### The Triple-Track Strategy

1.  **Track 1: Operational Mirror (Hot):** Captured in real-time by **Debezium** and **Kafka**. This provides a sub-second mirror of the source in the `public` schema for live dashboards and operational lookups.
2.  **Track 2: Analytical Warehouse (Warehouse):** Managed by **Airflow** and **dbt**. Snapshots are extracted daily and transformed into the `analytics` schema (Gold layer). This provides the "Cleaned Truth" for financial and business reporting.
3.  **Track 3: Big Data Lakehouse (Scale):** Managed by **Airflow** and **Spark**. Raw files are processed into **Apache Iceberg** tables (Silver layer) in MinIO. This track is designed to handle billions of rows that would be too expensive to store in the relational warehouse.

## Technology Stack

| Layer | Component |
|---|---|
| **Orchestration** | Apache Airflow 2.8 |
| **CDC / Streaming** | Debezium 2.5 & Confluent Kafka 7.5 |
| **Object Storage** | MinIO (S3-compatible) |
| **Transformation** | dbt-core 1.7 (Incremental Models) |
| **Batch Compute** | Apache Spark 3.5 & Apache Iceberg 1.4 |
| **Data Warehouse** | PostgreSQL 15 |
| **Monitoring** | Prometheus & Grafana |

## Agentic AI Integration

This repository features state-of-the-art **Model Context Protocol (MCP)** integration, allowing LLM coding agents (like Claude Desktop or Cursor) to act natively as Data Engineers.

Through the custom `dbt-mcp` server located in this repository, the AI Agent interacts directly with the production environment:
- **Zero-Guessing Architecture:** The Agent explicitly reads and writes real SQL code and schema metadata directly from the Destination Data Warehouse. It never has to "guess" or "hallucinate" table structures because it has live, native database access.
- **Autonomous Validation:** It can natively trigger `dbt test` against the live PostgreSQL database to instantly verify its own code changes.
- **Human-in-the-Loop Self-Healing:** By hooking the MCP directly into the warehouse, the Agent can analyze live pipeline failures and explicitly write and test its own SQL patches. However, it strictly requires human approval before any code is committed or applied, guaranteeing complete control and security without requiring human copy-pasting.

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

### Airflow DAGs

1. **`ingest_source_to_bronze`** *(The Ingestion Engine)*
   - Parallel flow: Simultaneously extracts data to **MinIO Bronze** (for the Lakehouse) and **Postgres public** (for the Warehouse).
   - This ensures both tracks start with the exact same raw snapshot.
2. **`spark_transform_silver`** *(The Lakehouse Engine)*
   - High-scale Spark jobs that process billions of rows from Bronze into **Apache Iceberg** tables.
   - Includes maintenance tasks like Z-Ordering and compaction for extreme performance.
3. **`dbt_transformations`** *(The Warehouse Engine)*
   - dbt models that take the raw mirrored data and apply business logic to create the **Gold** layer in the `analytics` schema.

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
make dbt-run             # Execute dbt transformation (Incremental)
make dbt-test            # Execute dbt validation tests
```
