# Unified Logistics Data Platform

[![Pipeline](https://github.com/YOUR_USERNAME/unified-logistics-platform/actions/workflows/pipeline.yml/badge.svg)](https://github.com/YOUR_USERNAME/unified-logistics-platform/actions)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

> End-to-end data platform for logistics operations: Fleet Telematics + Shipment Tracking + Last-Mile Delivery.

---

## 🎯 What This Project Demonstrates

| Category | Skills |
|----------|--------|
| **Data Sourcing** | Kafka producers, event simulation, CDC patterns |
| **Data Modeling** | Dimensional modeling, star schema, SCD Type 2 |
| **Stream Processing** | Spark Structured Streaming, stateful processing |
| **Batch Processing** | Spark batch jobs, trip/journey reconstruction |
| **Orchestration** | Airflow DAGs, dependencies, alerting |
| **Data Quality** | Great Expectations, dbt tests |
| **Transformations** | dbt models, incremental processing |
| **Geospatial** | H3 hexagonal indexing, geofencing |
| **Storage** | Delta Lake, partitioning, ACID transactions |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                       │
│   GPS Devices      Scanner Apps       Delivery App                          │
│   (Vehicles)       (Hub Workers)      (Agents)                              │
└───────────┬──────────────┬────────────────┬─────────────────────────────────┘
            │              │                │
            ▼              ▼                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        KAFKA CLUSTER                                         │
│   vehicle_positions | shipment_events | agent_positions | delivery_events   │
└───────────┬──────────────┬────────────────┬─────────────────────────────────┘
            │              │                │
            ▼              ▼                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SPARK STRUCTURED STREAMING                                │
│   • GPS validation & H3 indexing                                            │
│   • Geofence detection                                                       │
│   • Shipment status materialization                                          │
│   • Stop detection for agents                                               │
└───────────┬──────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       DELTA LAKE (Bronze → Silver → Gold)                    │
└───────────┬──────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SPARK BATCH + dbt                                    │
│   • Trip reconstruction            • dbt staging models                     │
│   • Journey reconstruction         • dbt mart models                        │
│   • Aggregations                   • dbt tests                              │
└───────────┬──────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DATA WAREHOUSE (DuckDB)                              │
│   Dimensional model: Facts + Dimensions for analytics                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📊 Three Integrated Modules

### 1. Fleet Telematics
- **Data:** GPS positions (10 sec), OBD telemetry (30 sec)
- **Processing:** Geofence detection, driving events, route deviation
- **Output:** Trips, driver scores, vehicle utilization

### 2. Shipment Tracking  
- **Data:** Package scan events at each hub
- **Processing:** Status materialization, SLA monitoring, stuck detection
- **Output:** Journeys, hub throughput, SLA compliance

### 3. Last-Mile Delivery
- **Data:** Delivery agent GPS, delivery attempts
- **Processing:** Stop detection, failure analysis
- **Output:** Agent shifts, area performance, delivery metrics

---

## 🚀 Quick Start

```bash
# Clone
git clone https://github.com/YOUR_USERNAME/unified-logistics-platform.git
cd unified-logistics-platform

# Setup
make setup
source venv/bin/activate

# Start infrastructure
make infra-up
# Wait for services to start (~30 seconds)

# Start data simulators (in separate terminal)
make simulate

# Start streaming jobs (in separate terminal)
make stream

# After data accumulates, run batch processing
make batch

# Run dbt models
make dbt-run

# View Airflow UI
open http://localhost:8080  # admin/admin
```

---

## 📁 Project Structure

```
unified-logistics-platform/
├── src/
│   ├── simulators/        # Data generators
│   ├── streaming/         # Spark Streaming jobs
│   ├── batch/            # Spark batch jobs
│   └── quality/          # Great Expectations
├── dbt_logistics/         # dbt models
├── dags/                  # Airflow DAGs
├── infrastructure/        # Docker Compose files
├── tests/
├── notebooks/
└── docs/
```

---

## 📈 Data Model

### Fact Tables

| Module | Fact Table | Description |
|--------|------------|-------------|
| Fleet | fact_vehicle_positions | GPS readings (high volume) |
| Fleet | fact_trips | Reconstructed journeys |
| Fleet | fact_driving_events | Speeding, harsh braking |
| Shipment | fact_shipment_events | All scan events |
| Shipment | fact_sla_tracking | SLA compliance |
| Shipment | fact_hub_throughput | Hub metrics |
| Delivery | fact_delivery_attempts | Each delivery try |
| Delivery | fact_agent_shifts | Daily agent metrics |
| Delivery | fact_area_performance | Zone-level stats |

### Dimension Tables

- dim_time, dim_geography (H3), dim_hubs
- dim_vehicles, dim_drivers
- dim_delivery_agents
- dim_shipments, dim_customers, dim_sellers

---

## 🛠️ Technology Stack

| Layer | Technology |
|-------|------------|
| Messaging | Apache Kafka |
| Stream Processing | Spark Structured Streaming |
| Batch Processing | Apache Spark |
| Storage | Delta Lake on MinIO (S3-compatible) |
| Transformations | dbt |
| Orchestration | Apache Airflow |
| Data Quality | Great Expectations |
| Geospatial | H3 (Uber's hexagonal grid) |
| Warehouse | DuckDB |
| Infrastructure | Docker Compose |

---

## 🇮🇳 India Market Relevance

| Company | How This Applies |
|---------|------------------|
| **Delhivery** | Fleet tracking, shipment tracking, last-mile |
| **Porter** | Fleet telematics, trip analytics |
| **Ecom Express** | Hub throughput, SLA monitoring |
| **Shadowfax** | Delivery agent optimization |
| **Swiggy/Zomato** | Last-mile delivery analytics |
| **Ola/Uber** | Fleet tracking, driver behavior |

---

## 📚 Documentation

- [Architecture Deep Dive](docs/architecture.md)
- [Data Dictionary](docs/data_dictionary.md)
- [Setup Guide](docs/setup_guide.md)
- [dbt Model Documentation](http://localhost:8001) (run `make dbt-docs`)

---

## 👤 Author

**Your Name** - [GitHub](https://github.com/your_username) | [LinkedIn](https://linkedin.com/in/your_username)
