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
  TRACK B:      TRACK A:    TRACK C:
 OPERATIONAL   ANALYTICAL   BIG DATA
  (Mirror)    (Warehouse)  (Lakehouse)
      │           │           │
┌─────▼─────┐     │     ┌─────▼─────┐
│ Debezium  │     │     │  Airflow  │ (The Orchestrator)
└─────┬─────┘     │     └─────┬─────┘
      │           │           │
┌─────▼─────┐     │     ┌─────▼─────┐
│   Kafka   │     │     │  MinIO    │ (Bronze Bucket)
└─────┬─────┘     │     └─────┬─────┘
      │      ┌────▼────┐      │
┌─────▼─────┐│ Airflow │┌─────▼─────┐
│ Real-Time │└────┬────┘│  Spark    │ (PySpark / Iceberg)
│   Apps    │     │     └─────┬─────┘
└───────────┘     │           │
            ┌─────▼─────┐     │
            │    raw    │ ◀───┘
            │ (_source) │
            └─────┬─────┘
                  │
            ┌─────▼─────┐
            │    dbt    │ (The Transformation Brain)
            └─────┬─────┘
                  ▼
            ┌────────────┐
            │    int     │ (Integration Layer: _clean)
            └─────┬──────┘
                  ▼
            ┌────────────┐
            │    prs     │ (Presentation Layer: v_*)
            └─────┬──────┘
                  │
            ┌─────▼─────┐
            │ Metabase  │ (BI Dashboard)
            └───────────┘
```

### The Triple-Track Strategy

1.  **Track A: Analytical Warehouse (BI / Reporting):** Managed by **Airflow** and **dbt**. Snapshots are extracted daily and upserted into the `raw` schema. dbt cleans the data into the `int` schema, and aggregates it into the `prs` schema. This provides the "Cleaned Truth" for financial and business reporting via Metabase.
2.  **Track B: Operational Streaming (Real-Time CDC):** Captured in real-time by **Debezium** and **Kafka**. This provides a sub-second mirror of the source database changes for live downstream event-driven microservices.
3.  **Track C: Big Data Lakehouse (Scale):** Managed by **Airflow**, **MinIO**, and **Spark**. Raw Parquet files are processed into **Apache Iceberg** tables (Silver catalog). This track is designed to handle massive-scale analytical workloads using distributed computing.

## Database Schema Structure

The Destination Data Warehouse (`destdb`) is strictly organized to ensure data quality and clear governance:
- **`raw` Schema:** Receives raw, messy data directly from the source system. Tables strictly follow the `_source` suffix (e.g., `raw.orders_source`).
- **`int` Schema:** The integration layer where data is cleaned, filtered, and deduplicated. Tables strictly follow the `_clean` suffix (e.g., `int.orders_clean`).
- **`prs` Schema:** The presentation layer exposing final, aggregated business views (e.g., `prs.v_daily_revenue`). Only this schema is exposed to BI tools.

## Technology Stack

| Layer | Component |
|---|---|
| **Orchestration** | Apache Airflow 2.8 |
| **CDC / Streaming** | Debezium 2.5 & Confluent Kafka 7.5 |
| **Object Storage** | MinIO (S3-compatible) |
| **Transformation** | dbt-core 1.7 (Incremental Models) |
| **Batch Compute** | Apache Spark 3.5 & Apache Iceberg 1.4 |
| **Data Warehouse** | PostgreSQL 15 |
| **BI / Dashboards** | Metabase |

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
| **Metabase** | http://localhost:3030 | (Setup Required) |
| **Spark Master** | http://localhost:8081 | — |
| **Kafka Connect** | http://localhost:8083 | — |

## Data Pipelines

### Airflow DAGs

1. **`ingest_source_to_bronze`** *(The Ingestion Engine)*
   - Parallel flow: Simultaneously extracts data to **MinIO Bronze** (for the Lakehouse) and **Postgres `raw`** (for the Warehouse).
   - Once ingestion is complete, it directly triggers the `dbt_transformations` task group.
2. **`dbt_transformations`** *(The Transformation Engine)*
   - Automatically cleans raw data into the `int` schema, and builds presentation views in the `prs` schema.
3. **`spark_transform_silver`** *(The Lakehouse Engine)*
   - High-scale Spark jobs that process raw Parquet files from Bronze into **Apache Iceberg** tables.

### dbt Modeling Structure

```text
dbt/models/
├── int/
│   ├── customers_clean.sql
│   ├── order_items_clean.sql
│   ├── orders_clean.sql
│   └── products_clean.sql
└── prs/
    ├── v_customers.sql
    ├── v_daily_revenue.sql
    ├── v_orders.sql
    └── v_products.sql
```

## Project Operations

```bash
make up                  # Start infrastructure
make down                # Tear down infrastructure
make logs                # Tail aggregated container logs
make ps                  # Service health check
make seed                # Generate sample source data
make register-connector  # Initialize Debezium CDC connector
```
