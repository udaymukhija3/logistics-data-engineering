# =============================================================================
# Unified Logistics Data Platform - Makefile
# =============================================================================

.PHONY: setup infra-up infra-down simulate stream batch dbt-run dbt-test quality test lint help clean

# Configuration
PYTHON := python3
VENV := venv
KAFKA_BOOTSTRAP := localhost:9092
SPARK_PACKAGES := org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0

# =============================================================================
# HELP
# =============================================================================

help:
	@echo ""
	@echo "Unified Logistics Data Platform"
	@echo "================================"
	@echo ""
	@echo "Setup:"
	@echo "  make setup           - Create venv, install deps"
	@echo "  make infra-up        - Start all infrastructure (Kafka, Spark, Airflow)"
	@echo "  make infra-down      - Stop all infrastructure"
	@echo ""
	@echo "Data Generation:"
	@echo "  make simulate        - Run all data simulators"
	@echo "  make simulate-demo   - Run 2-minute demo simulation"
	@echo "  make simulate-fleet  - Run fleet GPS simulator only"
	@echo ""
	@echo "Processing:"
	@echo "  make stream          - Start Spark streaming jobs (Kafka -> Bronze)"
	@echo "  make batch           - Run all batch jobs"
	@echo "  make batch-trips     - Run trip reconstruction"
	@echo "  make batch-journeys  - Run journey reconstruction"
	@echo "  make batch-shifts    - Run agent shift aggregation"
	@echo ""
	@echo "dbt:"
	@echo "  make dbt-deps        - Install dbt packages"
	@echo "  make dbt-run         - Run all dbt models"
	@echo "  make dbt-test        - Run dbt tests"
	@echo "  make dbt-docs        - Generate and serve dbt docs"
	@echo ""
	@echo "Quality:"
	@echo "  make quality         - Run data quality checks"
	@echo "  make test            - Run pytest"
	@echo ""
	@echo "Demo:"
	@echo "  make demo            - Run end-to-end demo"
	@echo "  make dashboard       - Start Streamlit dashboard"
	@echo "  make notebook        - Start Jupyter notebook"
	@echo ""

# =============================================================================
# SETUP
# =============================================================================

setup:
	@echo "Creating virtual environment..."
	$(PYTHON) -m venv $(VENV)
	@echo "Installing dependencies..."
	. $(VENV)/bin/activate && pip install --upgrade pip
	. $(VENV)/bin/activate && pip install -r requirements.txt
	@echo ""
	@echo "Setup complete! Run: source venv/bin/activate"

setup-dev: setup
	. $(VENV)/bin/activate && pip install pytest pytest-cov black isort mypy jupyter

# =============================================================================
# INFRASTRUCTURE
# =============================================================================

infra-up:
	@echo "Starting Docker infrastructure..."
	docker-compose -f infrastructure/docker-compose.yml up -d
	@echo "Waiting for services to start (30s)..."
	@sleep 30
	@echo "Creating Kafka topics..."
	@bash infrastructure/kafka_topics.sh || true
	@echo ""
	@echo "Infrastructure started:"
	@echo "  Kafka UI: http://localhost:8090"
	@echo "  Spark Master: http://localhost:8081"
	@echo "  Airflow: http://localhost:8080 (admin/admin)"
	@echo "  MinIO: http://localhost:9001 (minioadmin/minioadmin)"

infra-down:
	docker-compose -f infrastructure/docker-compose.yml down

infra-status:
	docker-compose -f infrastructure/docker-compose.yml ps

infra-logs:
	docker-compose -f infrastructure/docker-compose.yml logs -f

# =============================================================================
# SIMULATORS
# =============================================================================

simulate:
	@echo "Starting all simulators..."
	. $(VENV)/bin/activate && python -m src.simulators.run_all \
		--kafka $(KAFKA_BOOTSTRAP) \
		--vehicles 50 \
		--agents 100 \
		--shipments-rate 10

simulate-demo:
	@echo "Running 2-minute demo simulation..."
	. $(VENV)/bin/activate && python -m src.simulators.run_all \
		--kafka $(KAFKA_BOOTSTRAP) \
		--vehicles 20 \
		--agents 50 \
		--shipments-rate 5 \
		--duration 120

simulate-fleet:
	. $(VENV)/bin/activate && python -m src.simulators.vehicle_simulator \
		--kafka $(KAFKA_BOOTSTRAP) \
		--vehicles 50

simulate-shipments:
	. $(VENV)/bin/activate && python -m src.simulators.shipment_simulator \
		--kafka $(KAFKA_BOOTSTRAP) \
		--rate 10

simulate-delivery:
	. $(VENV)/bin/activate && python -m src.simulators.delivery_simulator \
		--kafka $(KAFKA_BOOTSTRAP) \
		--agents 100

# =============================================================================
# STREAMING
# =============================================================================

stream:
	@echo "Starting Spark streaming jobs..."
	. $(VENV)/bin/activate && spark-submit \
		--packages $(SPARK_PACKAGES) \
		src/streaming/bronze_ingestion.py \
		--kafka $(KAFKA_BOOTSTRAP) \
		--bronze-path data/bronze \
		--checkpoint-path data/checkpoints

stream-local:
	@echo "Starting streaming with local Spark..."
	. $(VENV)/bin/activate && python -m src.streaming.bronze_ingestion \
		--kafka $(KAFKA_BOOTSTRAP) \
		--bronze-path data/bronze \
		--checkpoint-path data/checkpoints

# =============================================================================
# BATCH PROCESSING
# =============================================================================

batch: batch-trips batch-journeys batch-shifts
	@echo "All batch jobs completed."

batch-trips:
	@echo "Running trip reconstruction..."
	. $(VENV)/bin/activate && spark-submit \
		--packages $(SPARK_PACKAGES) \
		src/batch/trip_reconstruction.py \
		--bronze-path data/bronze \
		--silver-path data/silver

batch-journeys:
	@echo "Running journey reconstruction..."
	. $(VENV)/bin/activate && spark-submit \
		--packages $(SPARK_PACKAGES) \
		src/batch/journey_reconstruction.py \
		--bronze-path data/bronze \
		--silver-path data/silver

batch-shifts:
	@echo "Running agent shift aggregation..."
	. $(VENV)/bin/activate && spark-submit \
		--packages $(SPARK_PACKAGES) \
		src/batch/agent_shift_aggregation.py \
		--bronze-path data/bronze \
		--silver-path data/silver

# Local batch (without spark-submit)
batch-local:
	. $(VENV)/bin/activate && python -m src.batch.trip_reconstruction --bronze-path data/bronze --silver-path data/silver
	. $(VENV)/bin/activate && python -m src.batch.journey_reconstruction --bronze-path data/bronze --silver-path data/silver
	. $(VENV)/bin/activate && python -m src.batch.agent_shift_aggregation --bronze-path data/bronze --silver-path data/silver

# =============================================================================
# DBT
# =============================================================================

dbt-deps:
	@echo "Installing dbt packages..."
	cd dbt_logistics && dbt deps --profiles-dir .

dbt-run: dbt-deps
	@echo "Running dbt models..."
	cd dbt_logistics && dbt run --profiles-dir .

dbt-run-staging:
	cd dbt_logistics && dbt run --select staging --profiles-dir .

dbt-run-intermediate:
	cd dbt_logistics && dbt run --select intermediate --profiles-dir .

dbt-run-marts:
	cd dbt_logistics && dbt run --select marts --profiles-dir .

dbt-test:
	@echo "Running dbt tests..."
	cd dbt_logistics && dbt test --profiles-dir .

dbt-docs:
	@echo "Generating dbt documentation..."
	cd dbt_logistics && dbt docs generate --profiles-dir .
	@echo "Starting dbt docs server at http://localhost:8001"
	cd dbt_logistics && dbt docs serve --port 8001 --profiles-dir .

dbt-clean:
	cd dbt_logistics && dbt clean --profiles-dir .

# =============================================================================
# QUALITY CHECKS
# =============================================================================

quality:
	@echo "Running data quality checks..."
	. $(VENV)/bin/activate && python -m src.quality.quality_checks --layer all --data-path data

quality-bronze:
	. $(VENV)/bin/activate && python -m src.quality.quality_checks --layer bronze --data-path data

quality-silver:
	. $(VENV)/bin/activate && python -m src.quality.quality_checks --layer silver --data-path data

# =============================================================================
# TESTING
# =============================================================================

test:
	@echo "Running tests..."
	. $(VENV)/bin/activate && pytest tests/ -v

test-cov:
	. $(VENV)/bin/activate && pytest tests/ -v --cov=src --cov-report=html

test-unit:
	. $(VENV)/bin/activate && pytest tests/unit/ -v

test-integration:
	. $(VENV)/bin/activate && pytest tests/integration/ -v

# =============================================================================
# CODE QUALITY
# =============================================================================

lint:
	. $(VENV)/bin/activate && python -m py_compile src/**/*.py

format:
	. $(VENV)/bin/activate && black src/ tests/
	. $(VENV)/bin/activate && isort src/ tests/

# =============================================================================
# DEMO
# =============================================================================

demo:
	@echo "============================================"
	@echo "   Running End-to-End Demo"
	@echo "============================================"
	@echo ""
	@echo "Step 1: Starting infrastructure..."
	@make infra-up || true
	@echo ""
	@echo "Step 2: Running simulation (2 min)..."
	@make simulate-demo &
	@sleep 30
	@echo ""
	@echo "Step 3: Running batch jobs..."
	@make batch-local || true
	@echo ""
	@echo "Step 4: Running quality checks..."
	@make quality || true
	@echo ""
	@echo "============================================"
	@echo "   Demo Complete!"
	@echo "============================================"

notebook:
	@echo "Starting Jupyter notebook..."
	. $(VENV)/bin/activate && jupyter notebook notebooks/

dashboard:
	@echo "Starting Streamlit dashboard at http://localhost:8501"
	. $(VENV)/bin/activate && streamlit run src/dashboard/app.py

# =============================================================================
# CLEANUP
# =============================================================================

clean:
	rm -rf __pycache__ .pytest_cache
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

clean-data:
	rm -rf data/bronze/* data/silver/* data/gold/*
	rm -rf data/checkpoints/*
	rm -rf data/quality_reports/*

clean-all: clean clean-data
	rm -rf $(VENV)
	rm -rf dbt_logistics/target dbt_logistics/dbt_packages
	docker-compose -f infrastructure/docker-compose.yml down -v 2>/dev/null || true

.DEFAULT_GOAL := help
