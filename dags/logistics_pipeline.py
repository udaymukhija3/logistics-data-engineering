"""
Daily Batch Processing DAG for Unified Logistics Platform.

Runs at 2 AM daily to:
1. Reconstruct trips from GPS data
2. Reconstruct shipment journeys
3. Aggregate agent shifts
4. Run dbt models
5. Run data quality checks
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Configuration
PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow")
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
SPARK_PACKAGES = "io.delta:delta-spark_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
DUCKDB_PATH = os.getenv("LOGISTICS_DUCKDB_PATH", f"{PROJECT_ROOT}/data/warehouse/logistics.duckdb")

# Default arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# DAG definition
with DAG(
    dag_id="logistics_daily_batch_processing",
    default_args=default_args,
    description="Daily batch processing for logistics platform",
    schedule_interval="0 2 * * *",  # 2 AM daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["logistics", "batch", "daily"],
) as dag:

    # ============================================
    # START
    # ============================================
    start = EmptyOperator(task_id="start")

    # ============================================
    # GET PROCESSING DATE
    # ============================================
    def get_processing_date(**context):
        """Get the date to process (yesterday)."""
        execution_date = context["ds"]
        processing_date = (
            datetime.strptime(execution_date, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")
        context["ti"].xcom_push(key="processing_date", value=processing_date)
        return processing_date

    get_date = PythonOperator(
        task_id="get_processing_date",
        python_callable=get_processing_date,
    )

    # ============================================
    # FLEET TELEMATICS BATCH JOBS
    # ============================================
    with TaskGroup(group_id="fleet_processing") as fleet_group:

        trip_reconstruction = BashOperator(
            task_id="trip_reconstruction",
            bash_command=f"""
                spark-submit \
                    --master {SPARK_MASTER} \
                    --packages {SPARK_PACKAGES} \
                    {PROJECT_ROOT}/src/batch/trip_reconstruction.py \
                    --date {{{{ ti.xcom_pull(key='processing_date') }}}} \
                    --bronze-path {PROJECT_ROOT}/data/bronze \
                    --silver-path {PROJECT_ROOT}/data/silver
            """,
        )

    # ============================================
    # SHIPMENT TRACKING BATCH JOBS
    # ============================================
    with TaskGroup(group_id="shipment_processing") as shipment_group:

        journey_reconstruction = BashOperator(
            task_id="journey_reconstruction",
            bash_command=f"""
                spark-submit \
                    --master {SPARK_MASTER} \
                    --packages {SPARK_PACKAGES} \
                    {PROJECT_ROOT}/src/batch/journey_reconstruction.py \
                    --date {{{{ ti.xcom_pull(key='processing_date') }}}} \
                    --bronze-path {PROJECT_ROOT}/data/bronze \
                    --silver-path {PROJECT_ROOT}/data/silver
            """,
        )

    # ============================================
    # LAST-MILE BATCH JOBS
    # ============================================
    with TaskGroup(group_id="delivery_processing") as delivery_group:

        agent_shift_agg = BashOperator(
            task_id="agent_shift_aggregation",
            bash_command=f"""
                spark-submit \
                    --master {SPARK_MASTER} \
                    --packages {SPARK_PACKAGES} \
                    {PROJECT_ROOT}/src/batch/agent_shift_aggregation.py \
                    --date {{{{ ti.xcom_pull(key='processing_date') }}}} \
                    --bronze-path {PROJECT_ROOT}/data/bronze \
                    --silver-path {PROJECT_ROOT}/data/silver
            """,
        )

    # ============================================
    # DBT TRANSFORMATIONS
    # ============================================
    with TaskGroup(group_id="dbt_transformations") as dbt_group:

        dbt_bootstrap_sources = BashOperator(
            task_id="dbt_bootstrap_sources",
            bash_command=(
                f"PYTHONPATH={PROJECT_ROOT} python {PROJECT_ROOT}/scripts/bootstrap_duckdb_sources.py "
                f"--data-path {PROJECT_ROOT}/data --db-path {DUCKDB_PATH}"
            ),
        )

        dbt_parse = BashOperator(
            task_id="dbt_parse",
            bash_command=(
                f"cd {PROJECT_ROOT}/dbt_logistics && "
                f"LOGISTICS_DUCKDB_PATH={DUCKDB_PATH} dbt parse --profiles-dir ."
            ),
        )

        dbt_run_staging = BashOperator(
            task_id="dbt_run_staging",
            bash_command=(
                f"cd {PROJECT_ROOT}/dbt_logistics && "
                f"LOGISTICS_DUCKDB_PATH={DUCKDB_PATH} dbt run --select staging --profiles-dir ."
            ),
        )

        dbt_run_intermediate = BashOperator(
            task_id="dbt_run_intermediate",
            bash_command=(
                f"cd {PROJECT_ROOT}/dbt_logistics && "
                f"LOGISTICS_DUCKDB_PATH={DUCKDB_PATH} dbt run --select intermediate --profiles-dir ."
            ),
        )

        dbt_run_marts = BashOperator(
            task_id="dbt_run_marts",
            bash_command=(
                f"cd {PROJECT_ROOT}/dbt_logistics && "
                f"LOGISTICS_DUCKDB_PATH={DUCKDB_PATH} dbt run --select marts --profiles-dir ."
            ),
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=(
                f"cd {PROJECT_ROOT}/dbt_logistics && "
                f"LOGISTICS_DUCKDB_PATH={DUCKDB_PATH} dbt test --profiles-dir ."
            ),
        )

        (
            dbt_bootstrap_sources
            >> dbt_parse
            >> dbt_run_staging
            >> dbt_run_intermediate
            >> dbt_run_marts
            >> dbt_test
        )

    # ============================================
    # DATA QUALITY CHECKS
    # ============================================
    with TaskGroup(group_id="quality_checks") as quality_group:

        quality_bronze = BashOperator(
            task_id="quality_check_bronze",
            bash_command=f"python {PROJECT_ROOT}/src/quality/quality_checks.py --layer bronze --data-path {PROJECT_ROOT}/data",
        )

        quality_silver = BashOperator(
            task_id="quality_check_silver",
            bash_command=f"python {PROJECT_ROOT}/src/quality/quality_checks.py --layer silver --data-path {PROJECT_ROOT}/data",
        )

        [quality_bronze, quality_silver]

    # ============================================
    # ALERTING / REPORTING
    # ============================================
    def send_completion_alert(**context):
        """Send Slack alert on completion."""
        processing_date = context["ti"].xcom_pull(key="processing_date")
        # In production, would send to Slack
        print(f"Daily batch processing completed for {processing_date}")

    completion_alert = PythonOperator(
        task_id="send_completion_alert",
        python_callable=send_completion_alert,
        trigger_rule="all_success",
    )

    # ============================================
    # END
    # ============================================
    end = EmptyOperator(task_id="end")

    # ============================================
    # TASK DEPENDENCIES
    # ============================================
    start >> get_date

    # Parallel processing of all three modules
    get_date >> [fleet_group, shipment_group, delivery_group]

    # dbt runs after all Spark jobs
    [fleet_group, shipment_group, delivery_group] >> dbt_group

    # Quality checks after dbt
    dbt_group >> quality_group

    # Alert and end
    quality_group >> completion_alert >> end
