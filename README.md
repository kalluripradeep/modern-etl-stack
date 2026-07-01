# Modern ETL Infrastructure

A comprehensive ETL stack demonstrating the integration of open-source data engineering tools. This project features both **real-time CDC (Change Data Capture)** via Kafka/Debezium and **high-scale batch processing** pipelines orchestrating data movement between a staging relational database, a data lake, and a destination database.

## Architecture

The system utilizes a **Definitive "Three-Track" Architecture**. This design separates highspeed operational mirrors, business-critical analytics, and massive historical archives into independent, parallel tracks.

![Architecture Diagram](docs/images/architecture.png)

## Data Pipelines

Pipelines use proven modern infrastructures (refer to figure): Airflow for complex orchestration, Spark for large datasets processing, DBT for transformations, Kafka for messaging, and MCP server for natural language queries. Business Intelligence (BI) tools for humans and agentic AI tools can be hooked up but will be covered in future work.

### 1. Data Pipe #1: Analytical Warehouse - Batch Analysis
Ideal for lower data urgency and relative operational simplicity. The objective is to off-load data from operational databases to analytics databases, thereby maintaining the performance SLAs of the operational database. This data transfer is managed by Airflow. Snapshots are extracted periodically from destination Postgres and DBT transforms into an analytics schema (Gold layer). This provides high data quality for analytics reporting, business intelligence tools, training AI models, and inference AI models. It also applies when processing large historical volumes but data transfer resources are minimal. Advantages are simpler architecture for deployment and debugging and lower infrastructure resource consumption.

### 2. Data Pipe #2: Lakehouse - Multiple sources and Multiple Query Engines
Apache Iceberg is used as a lakehouse for ingesting data from operational Postgres. This pipeline is ideal when ingesting data from multiple operational databases, via streaming or batch, as well as data from asynchronous events that affect decisions. Second, multiple groups need to query the lakehouse for analytics via diverse query engines. A lakehouse maintains data integrity and hence the quality of data. Supporting multiple input sources simultaneously while making data available for multiple agents addresses data urgency without blocking any specific entity.

This pipe transfers data in chunked micro-batches from operational Postgres to Apache Iceberg lakehouse. This transfer is managed by Airflow and chunking saves memory. Spark processes data in Apache Iceberg tables (Silver layer) in MinIO. This pipe is designed to handle billions of rows that would be too expensive to store and process for analytics in an operational database. Apache Iceberg offers a balance of data urgency while maintaining high data access by multiple entities.

### 3. Data Pipe #3: Operational Hot Mirror - Real-Time Analysis
Captures real-time data changes using change data capture (CDC) with Debezium and Kafka. This provides a sub-second mirror of the source data in Postgres column store for live dashboards and operational monitoring while reducing performance impact on the operational database. Moreover near-real time data feeds to AI inference models allows for high urgency decisions and actions by humans and AI agents.

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

## Orchestration & Pipeline Details

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
