# Unified Logistics Data Platform

## What It Does

This repo models a logistics data platform across fleet telematics, shipment tracking, and last-mile delivery. It defines simulator-based event sources, a Spark Bronze/Silver pipeline, dbt warehouse models, Airflow orchestration, a custom quality framework, and a Streamlit dashboard. The verified end-to-end path today is sample-mode: generate or normalize sample parquet, validate it with the quality framework, and inspect the resulting Bronze/Silver artifacts plus JSON reports.  
Evidence: `README.md`, `src/simulators/`, `src/streaming/bronze_ingestion.py`, `src/batch/`, `dbt_logistics/`, `dags/logistics_pipeline.py`, `src/dashboard/app.py`, `/tmp/logistics_demo_tiny/quality_reports/quality_all_20260310_110123.json`

## Tech Stack

- Python 3.10+  
- Apache Kafka  
- Spark Structured Streaming + Spark batch  
- Delta Lake for intended Bronze/Silver runtime writes  
- dbt Core + dbt-duckdb  
- DuckDB for local analytics / warehouse modeling  
- Airflow for orchestration  
- Streamlit + Plotly for serving  
- pytest for tests  
- Docker Compose for local infra  
Evidence: `README.md`, `requirements.txt`, `infrastructure/docker-compose.yml`, `dbt_logistics/dbt_project.yml`, `dbt_logistics/profiles.yml`, `pytest.ini`

## What Runs End-to-End

What I verified locally:
- `pytest -q` passed 77 tests.  
- A tiny sample-mode pipeline produced Bronze and Silver parquet outputs under `/tmp/logistics_demo_tiny`.  
- `python -m src.quality.quality_checks --layer all --data-path /tmp/logistics_demo_tiny ...` passed 31/31 checks and wrote a JSON report.  

What is present but not yet release-proof locally:
- Kafka -> Spark streaming -> Delta Bronze/Silver  
- dbt marts in DuckDB  
- Airflow DAG execution through the full stack  
Evidence: `tests/`, `src/quality/quality_checks.py`, `/tmp/logistics_demo_tiny/bronze/vehicle_positions/sample_data.parquet`, `/tmp/logistics_demo_tiny/silver/fleet/trips/sample_data.parquet`, `/tmp/logistics_demo_tiny/quality_reports/quality_all_20260310_110123.json`, `src/streaming/bronze_ingestion.py`, `dbt_logistics/profiles.yml`, `dags/logistics_pipeline.py`

## 5 Strongest Proven Highlights

1. Typed event contracts for Bronze ingestion, with explicit schemas per topic and validation flags.  
Evidence: `src/streaming/bronze_ingestion.py`, `src/domain/constants.py`

2. Backfill-ready batch design, with date-parameterized Spark jobs and Airflow wiring for `processing_date`.  
Evidence: `src/batch/trip_reconstruction.py`, `src/batch/journey_reconstruction.py`, `src/batch/agent_shift_aggregation.py`, `dags/logistics_pipeline.py`

3. Portable data quality framework that runs on DuckDB or Spark and emits durable JSON reports.  
Evidence: `src/quality/quality_checks.py`, `tests/integration/test_quality_pipeline_integration.py`, `/tmp/logistics_demo_tiny/quality_reports/quality_all_20260310_110123.json`

4. Warehouse-style modeling with explicit fact grain, surrogate keys, and dbt tests.  
Evidence: `dbt_logistics/models/marts/fleet/fct_trips.sql`, `dbt_logistics/models/marts/shipment/fct_shipments.sql`, `dbt_logistics/models/marts/delivery/fct_agent_daily.sql`, `dbt_logistics/models/marts/_mart_models.yml`

5. Proven local demo path with real artifacts: test pass, generated Bronze/Silver files, and a 31/31 quality report.  
Evidence: `tests/`, `/tmp/logistics_demo_tiny/bronze/vehicle_positions/sample_data.parquet`, `/tmp/logistics_demo_tiny/silver/fleet/trips/sample_data.parquet`, `/tmp/logistics_demo_tiny/quality_reports/quality_all_20260310_110123.json`

## Demo Instructions

```bash
cd /Users/udaymukhija/logistics

python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt

export PYTHONPATH=/Users/udaymukhija/logistics
export DEMO_ROOT=/tmp/logistics_demo_tiny

python - <<'PY'
from pathlib import Path
import os, shutil
import pandas as pd
import scripts.generate_sample_data as g

root = Path(os.environ["DEMO_ROOT"])
if root.exists():
    shutil.rmtree(root)
root.mkdir(parents=True, exist_ok=True)

g.DATA_DIR = root
vehicle_df = g.generate_vehicle_positions(num_vehicles=5, positions_per_vehicle=20)
shipment_df = g.generate_shipment_events(num_shipments=25)
delivery_df = g.generate_delivery_events(num_agents=8, deliveries_per_agent=4)
g.generate_silver_trips(vehicle_df)
g.generate_silver_journeys(shipment_df)
g.generate_silver_agent_shifts(delivery_df)
g.generate_quality_report()

delivery = pd.read_parquet(root / "bronze" / "delivery_events" / "sample_data.parquet")
agent_positions = delivery[["agent_id", "latitude", "longitude"]].copy()
agent_positions.insert(0, "event_id", [f"ap_{i}" for i in range(len(agent_positions))])
(root / "bronze" / "agent_positions").mkdir(parents=True, exist_ok=True)
agent_positions.to_parquet(root / "bronze" / "agent_positions" / "sample_data.parquet", index=False)

trips_path = root / "silver" / "fleet" / "trips" / "sample_data.parquet"
trips = pd.read_parquet(trips_path)
trips["total_distance_km"] = trips.get("total_distance_km", trips["distance_km"])
trips.to_parquet(trips_path, index=False)
PY

pytest -q

python -m src.quality.quality_checks \
  --layer all \
  --data-path "$DEMO_ROOT" \
  --output-path "$DEMO_ROOT/quality_reports"
```

Success criteria:
- `pytest -q` ends with `77 passed`
- quality summary shows `31` checks passed and `0` failed
- Bronze/Silver parquet files exist under `$DEMO_ROOT`  
Evidence: `scripts/generate_sample_data.py`, `tests/`, `src/quality/quality_checks.py`, `/tmp/logistics_demo_tiny/quality_reports/quality_all_20260310_110123.json`

## Proof Artifacts List

- Test run result from `pytest -q`  
- Quality report JSON: `/tmp/logistics_demo_tiny/quality_reports/quality_all_20260310_110123.json`  
- Bronze parquet outputs under `/tmp/logistics_demo_tiny/bronze/`  
- Silver parquet outputs under `/tmp/logistics_demo_tiny/silver/`  
- Optional Streamlit screenshot from `streamlit run src/dashboard/app.py`  
- If extending to the full stack: Airflow DAG screenshot, Spark UI screenshot, and live Delta `_delta_log` directories  
Evidence: `src/quality/quality_checks.py`, `/tmp/logistics_demo_tiny/quality_reports/quality_all_20260310_110123.json`, `/tmp/logistics_demo_tiny/bronze/vehicle_positions/sample_data.parquet`, `/tmp/logistics_demo_tiny/silver/fleet/trips/sample_data.parquet`, `src/dashboard/app.py`, `dags/logistics_pipeline.py`, `src/streaming/bronze_ingestion.py`
