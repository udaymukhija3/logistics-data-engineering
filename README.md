# Unified Logistics Data Platform

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange.svg)](https://spark.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.7+-green.svg)](https://www.getdbt.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> End-to-end data platform for logistics operations: **Fleet Telematics** + **Shipment Tracking** + **Last-Mile Delivery**

## 🎯 What This Project Demonstrates

| Category | Technologies & Skills |
|----------|----------------------|
| **Data Ingestion** | Kafka producers, event simulation, real-time streaming |
| **Stream Processing** | Spark Structured Streaming, stateful processing |
| **Batch Processing** | Spark batch jobs, trip/journey reconstruction |
| **Data Modeling** | Dimensional modeling, star schema, medallion architecture |
| **Orchestration** | Apache Airflow DAGs, task dependencies |
| **Data Quality** | Custom validation framework, dbt tests |
| **Transformations** | dbt models (staging → intermediate → marts) |
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
│                        APACHE KAFKA                                          │
│   vehicle_positions │ shipment_events │ agent_positions │ delivery_events   │
└───────────┬──────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SPARK STRUCTURED STREAMING                                │
│   • Schema validation              • Coordinate validation                  │
│   • Timestamp extraction           • Partitioning by date                   │
└───────────┬──────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       DELTA LAKE - BRONZE LAYER                              │
│   Raw events with ingestion metadata and data quality flags                 │
└───────────┬──────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SPARK BATCH JOBS                                     │
│   • Trip reconstruction            • Journey reconstruction                 │
│   • Agent shift aggregation        • Metrics calculation                    │
└───────────┬──────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       DELTA LAKE - SILVER LAYER                              │
│   Cleaned, validated, business-ready datasets                               │
└───────────┬──────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              dbt                                             │
│   Staging → Intermediate → Marts (Facts + Dimensions)                       │
└───────────┬──────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       DATA WAREHOUSE (DuckDB)                                │
│   Dimensional model optimized for analytics queries                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📊 Three Integrated Modules

### 1. Fleet Telematics
- **Data**: GPS positions (10 sec intervals), vehicle telemetry
- **Processing**: Trip reconstruction, driving event detection, route analysis
- **Output**: `fct_trips`, `fct_driver_performance`

### 2. Shipment Tracking
- **Data**: Package scan events at each hub in the network
- **Processing**: Journey reconstruction, SLA monitoring, bottleneck detection
- **Output**: `fct_shipments`, `fct_hub_daily`

### 3. Last-Mile Delivery
- **Data**: Delivery agent GPS, delivery attempts/completions
- **Processing**: Shift aggregation, performance metrics, zone analysis
- **Output**: `fct_agent_daily`, `fct_zone_daily`

---

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- Docker & Docker Compose
- Java 11+ (for Spark)

### Setup

```bash
# Clone
git clone https://github.com/YOUR_USERNAME/unified-logistics-platform.git
cd unified-logistics-platform

# Setup virtual environment
make setup
source venv/bin/activate

# Start infrastructure (Kafka, Spark, Airflow, MinIO)
make infra-up

# Run 2-minute demo simulation
make simulate-demo

# Run batch processing
make batch

# Run dbt models
make dbt-run

# Run data quality checks
make quality

# Explore data in Jupyter
make notebook
```

### Service URLs
| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Kafka UI | http://localhost:8090 | - |
| Spark UI | http://localhost:8081 | - |
| MinIO | http://localhost:9001 | minioadmin / minioadmin |

---

## 📁 Project Structure

```
unified-logistics-platform/
├── src/
│   ├── simulators/           # Data generators (Kafka producers)
│   │   ├── vehicle_simulator.py
│   │   ├── shipment_simulator.py
│   │   └── delivery_simulator.py
│   ├── streaming/            # Spark Streaming jobs
│   │   └── bronze_ingestion.py
│   ├── batch/                # Spark batch jobs
│   │   ├── trip_reconstruction.py
│   │   ├── journey_reconstruction.py
│   │   └── agent_shift_aggregation.py
│   └── quality/              # Data quality checks
│       └── quality_checks.py
├── dbt_logistics/            # dbt project
│   ├── models/
│   │   ├── staging/          # 1:1 with sources
│   │   ├── intermediate/     # Business logic
│   │   └── marts/            # Facts & dimensions
│   └── macros/
├── dags/                     # Airflow DAGs
├── infrastructure/           # Docker Compose
├── notebooks/                # Jupyter notebooks
├── tests/                    # Unit & integration tests
└── docs/                     # Documentation
```

---

## 📈 Data Model

### Fact Tables

| Module | Fact Table | Grain | Key Metrics |
|--------|------------|-------|-------------|
| Fleet | `fct_trips` | Per trip | distance, duration, speed, fuel |
| Fleet | `fct_driver_performance` | Per driver per day | safety_score, utilization |
| Shipment | `fct_shipments` | Per shipment | sla_status, journey_duration |
| Shipment | `fct_hub_daily` | Per hub per day | throughput, efficiency |
| Delivery | `fct_agent_daily` | Per agent per day | deliveries, success_rate |
| Delivery | `fct_zone_daily` | Per zone per day | volume, performance |

### Dimension Tables
- `dim_time` - Date dimension with fiscal year support
- `dim_hubs` - Hub master data (10 major Indian cities)

---

## 🛠️ Technology Stack

| Layer | Technology |
|-------|------------|
| **Messaging** | Apache Kafka |
| **Stream Processing** | Spark Structured Streaming |
| **Batch Processing** | Apache Spark 3.5 |
| **Storage** | Delta Lake |
| **Transformations** | dbt 1.7 |
| **Orchestration** | Apache Airflow |
| **Data Quality** | Custom framework + dbt tests |
| **Warehouse** | DuckDB |
| **Infrastructure** | Docker Compose |

---

## 🔧 Key Commands

```bash
# Infrastructure
make infra-up          # Start all services
make infra-down        # Stop all services
make infra-status      # Check service status

# Data Generation
make simulate          # Run all simulators
make simulate-demo     # Run 2-min demo

# Processing
make stream            # Start streaming jobs
make batch             # Run batch processing

# dbt
make dbt-run           # Run all models
make dbt-test          # Run tests
make dbt-docs          # Generate & serve docs

# Quality
make quality           # Run quality checks
make test              # Run pytest
```

---

## 📚 Documentation

- [Architecture Deep Dive](docs/logistics_platform_blueprint_part1.md)
- [Data Model Specification](docs/logistics_platform_blueprint_part2.md)
- [Implementation Guide](docs/logistics_platform_blueprint_part3.md)

---

## 🇮🇳 India Market Relevance

This platform architecture is directly applicable to:

| Company | Relevant Modules |
|---------|------------------|
| **Delhivery** | Fleet tracking, shipment tracking, last-mile |
| **Porter** | Fleet telematics, trip analytics |
| **Ecom Express** | Hub throughput, SLA monitoring |
| **Shadowfax** | Delivery agent optimization |
| **Swiggy/Zomato** | Last-mile delivery analytics |

---

## 🧪 Testing

```bash
# Run all tests
make test

# Run with coverage
make test-cov

# Run specific test categories
make test-unit
make test-integration
```

---

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

---

## 👤 Author

**Your Name**
- GitHub: [@your_username](https://github.com/your_username)
- LinkedIn: [your_username](https://linkedin.com/in/your_username)

---

<p align="center">
  Built with ❤️ for the data engineering community
</p>
