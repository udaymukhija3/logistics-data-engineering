# Unified Logistics Data Platform

This repository is an end-to-end logistics data engineering system that simulates operational events, ingests them in real time, transforms them into analytics-ready models, and serves insights through an interactive dashboard.

It is designed to demonstrate production-style engineering across streaming, batch processing, orchestration, data quality, testing, and deployment.

## Core Features

- Real-time ingestion from Kafka topics for fleet, shipment, and delivery domains
- Spark Structured Streaming pipeline into Bronze storage with schema validation and partitioned writes
- Batch reconstruction jobs for trips, shipment journeys, and delivery agent shifts
- dbt-based dimensional modeling from staging to marts
- Data quality checks with robust validation and report generation
- Contract-aligned sample data bundle that keeps dashboard, quality checks, and dbt in sync
- DuckDB source bootstrap that makes local dbt builds reproducible from parquet datasets
- Multi-simulator orchestration with guarded thread failures and graceful shutdown behavior
- Dashboard that supports both live data and sample data fallback for easy review
- Docker-based local infrastructure and lightweight dashboard deployment path

## Tech Stack

- Language: Python 3.10+
- Messaging: Apache Kafka
- Stream Processing: Apache Spark Structured Streaming
- Batch Processing: Apache Spark
- Storage Format: Delta Lake (runtime) and Parquet (sample datasets)
- Transformations: dbt (`dbt-core`, `dbt-duckdb`)
- Orchestration: Apache Airflow
- Warehouse and local analytics: DuckDB
- Visualization: Streamlit + Plotly
- Testing: pytest
- Infrastructure: Docker Compose

## Architecture Overview

The system is organized as three business modules that share a common platform:

- Fleet Telematics: vehicle GPS and telemetry, trip reconstruction, fleet analytics
- Shipment Tracking: hub events, journey reconstruction, SLA and bottleneck analysis
- Last-Mile Delivery: agent GPS, delivery outcomes, shift and zone performance

Pipeline flow:

1. Simulators publish events to Kafka topics.
2. Streaming jobs ingest Kafka topics to Bronze tables.
3. Batch jobs transform Bronze events into Silver datasets.
4. dbt models build analytics marts from curated data.
5. Quality checks validate critical data contracts and thresholds.
6. Streamlit dashboard surfaces operational KPIs and trends.

## Setup and Installation

### 1. Prerequisites

- Python 3.10 or later
- Docker and Docker Compose
- Java 11+ (required for Spark)

### 2. Clone the repository

```bash
git clone <your-repo-url>
cd logistics
```

### 3. Create environment and install dependencies

```bash
make setup
source venv/bin/activate
```

### 4. Configure environment variables

```bash
cp .env.example .env
```

Update `.env` before running infrastructure. At minimum, set secure values for:

- `POSTGRES_PASSWORD`
- `MINIO_ROOT_USER`
- `MINIO_ROOT_PASSWORD`
- `AIRFLOW__CORE__FERNET_KEY`
- `AIRFLOW_ADMIN_PASSWORD`

### 5. Start platform infrastructure

```bash
make infra-up
```

## Usage Examples

### End-to-end demo run (local)

```bash
# Build the verified sample bundle and warehouse views
make sample-data
make dbt-build

# Generate events for a short demo window
make simulate-demo

# Run batch jobs on produced data
make batch-local

# Run data quality checks
make quality

# Launch dashboard
make dashboard
```

### Domain-specific simulation

```bash
make simulate-fleet
make simulate-shipments
make simulate-delivery
```

### Streaming ingestion

```bash
make stream
```

### dbt transformations

```bash
make dbt-deps
make dbt-bootstrap
make dbt-run
make dbt-build
make dbt-test
```

### Tests

```bash
make test
make test-unit
make test-integration
```

## Verified Portfolio Path

The repo now has a fully reproducible local analytics path that does not depend on external dbt packages or a running warehouse bootstrap:

```bash
make sample-data
make dbt-build
make quality
make dashboard
```

What this proves locally:

- The sample bundle matches the Bronze and Silver data contracts used by the platform
- The quality framework passes end to end on the bundled datasets
- DuckDB source views can be bootstrapped directly from parquet
- `dbt build` completes successfully against the local warehouse file

## Service Endpoints (Local)

- Dashboard: `http://localhost:8501`
- Airflow: `http://localhost:8080`
- Kafka UI: `http://localhost:8090`
- Spark Master UI: `http://localhost:8081`
- MinIO Console: `http://localhost:9001`

Credentials are controlled through `.env`.

## Project Structure

```text
src/
  batch/              Spark batch transformations
  dashboard/          Streamlit application
  domain/             Shared constants
  quality/            Data quality framework
  simulators/         Event generators + orchestrator
  streaming/          Structured streaming ingestion
  utils/              Shared utility modules

dbt_logistics/        dbt project (staging/intermediate/marts)
dags/                 Airflow DAG definitions
infrastructure/       Docker Compose and infra scripts
data/sample/          Sample datasets used by dashboard and integration checks
tests/                Unit and integration tests
```

## Architectural Decisions

- Medallion-style separation (`Bronze -> Silver -> Marts`) keeps ingestion concerns isolated from business logic and analytics modeling.
- Spark is used for both streaming and batch to keep execution semantics consistent across real-time and scheduled processing.
- Domain constants and validation helpers are centralized to reduce drift between simulators, quality checks, and downstream transformations.
- Simulator orchestration uses failure guards to stop all modules when one crashes, preventing silent partial data generation.
- Data quality checks support both Spark and DuckDB backends so validation can run in lightweight local/deployment environments.
- The sample-data bundle is generated from canonical schemas so dashboard, quality, and dbt demos stay aligned.
- DuckDB source bootstrapping turns filesystem parquet into dbt-readable sources with no manual warehouse prep.
- The dashboard supports sample-data fallback to remain usable in portfolio and cloud demo contexts without full infrastructure.

## Deployment Notes

- Dashboard-only deployment is supported via:
  - `requirements-streamlit.txt`
  - multi-stage `Dockerfile` (`dashboard` stage)
  - `render.yaml`
- Full platform execution is intended for local Docker Compose or equivalent infrastructure environments.

## Additional Documentation

- `docs/logistics_platform_blueprint_part1.md`
- `docs/logistics_platform_blueprint_part2.md`
- `docs/logistics_platform_blueprint_part3.md`

## License

MIT License.
