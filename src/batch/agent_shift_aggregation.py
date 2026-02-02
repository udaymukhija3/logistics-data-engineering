"""
Agent Shift Aggregation - Batch job to aggregate delivery agent daily metrics.

This job:
1. Reads agent positions and delivery events from Bronze layer
2. Aggregates daily shift metrics per agent
3. Calculates performance metrics (deliveries/hour, success rate)
4. Identifies top performers and areas for improvement
5. Writes shift data to Silver layer
"""

import argparse
import logging
import math
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AgentShiftAggregator:
    """
    Aggregates delivery agent shift data and performance metrics.

    Creates:
    - Daily shift summaries per agent
    - Performance metrics (deliveries/hour, success rate, avg time per delivery)
    - Zone-level aggregations
    - Agent rankings
    """

    def __init__(
        self,
        spark: SparkSession = None,
        bronze_path: str = "data/bronze",
        silver_path: str = "data/silver",
    ):
        self.bronze_path = bronze_path
        self.silver_path = silver_path

        if spark:
            self.spark = spark
        else:
            self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Delta Lake support."""
        return (
            SparkSession.builder
            .appName("AgentShiftAggregation")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate()
        )

    def _read_agent_positions(self, date: str = None) -> DataFrame:
        """Read agent positions from Bronze layer."""
        path = f"{self.bronze_path}/agent_positions"
        df = self.spark.read.format("delta").load(path)

        if date:
            df = df.filter(F.col("event_date") == date)

        return df

    def _read_delivery_events(self, date: str = None) -> DataFrame:
        """Read delivery events from Bronze layer."""
        path = f"{self.bronze_path}/delivery_events"
        df = self.spark.read.format("delta").load(path)

        if date:
            df = df.filter(F.col("event_date") == date)

        return df

    def _calculate_distance_traveled(self, positions: DataFrame) -> DataFrame:
        """Calculate total distance traveled by each agent per day."""

        @F.udf(DoubleType())
        def haversine(lat1, lng1, lat2, lng2):
            if any(v is None for v in [lat1, lng1, lat2, lng2]):
                return 0.0

            R = 6371  # Earth's radius in km
            lat1_rad, lat2_rad = math.radians(lat1), math.radians(lat2)
            delta_lat = math.radians(lat2 - lat1)
            delta_lng = math.radians(lng2 - lng1)

            a = (math.sin(delta_lat/2)**2 +
                 math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lng/2)**2)
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
            return R * c

        # Window for ordering positions
        agent_window = Window.partitionBy("agent_id", "event_date").orderBy("event_timestamp")

        positions = positions.withColumn(
            "prev_lat",
            F.lag("latitude").over(agent_window)
        ).withColumn(
            "prev_lng",
            F.lag("longitude").over(agent_window)
        ).withColumn(
            "segment_distance_km",
            haversine(
                F.col("prev_lat"), F.col("prev_lng"),
                F.col("latitude"), F.col("longitude")
            )
        )

        # Aggregate distance per agent per day
        distance_agg = positions.groupBy(
            "agent_id",
            "event_date"
        ).agg(
            F.sum("segment_distance_km").alias("total_distance_km"),
            F.count("*").alias("position_count")
        )

        return distance_agg

    def _aggregate_shift_metrics(self, positions: DataFrame) -> DataFrame:
        """Aggregate basic shift metrics from positions."""
        shifts = positions.groupBy(
            "agent_id",
            "zone_id",
            "vehicle_type",
            "event_date"
        ).agg(
            F.min("event_timestamp").alias("shift_start_time"),
            F.max("event_timestamp").alias("shift_end_time"),
            F.count("*").alias("position_count"),
            F.avg("speed_kmh").alias("avg_speed_kmh"),
            F.max("speed_kmh").alias("max_speed_kmh"),
            F.sum(F.when(F.col("is_at_stop"), 1).otherwise(0)).alias("stop_count"),
            F.avg("battery_pct").alias("avg_battery_pct"),
            F.min("battery_pct").alias("min_battery_pct"),
            F.last("completed_today").alias("completed_deliveries"),
            F.last("failed_today").alias("failed_deliveries"),
            F.last("pending_orders").alias("pending_at_end"),
        )

        # Calculate shift duration
        shifts = shifts.withColumn(
            "shift_duration_hours",
            (F.unix_timestamp("shift_end_time") - F.unix_timestamp("shift_start_time")) / 3600
        )

        return shifts

    def _aggregate_delivery_metrics(self, deliveries: DataFrame) -> DataFrame:
        """Aggregate delivery event metrics."""
        delivery_agg = deliveries.groupBy(
            "agent_id",
            F.to_date("event_timestamp").alias("event_date")
        ).agg(
            F.count("*").alias("total_delivery_events"),
            F.sum(F.when(F.col("event_type") == "DELIVERED", 1).otherwise(0)).alias("successful_deliveries"),
            F.sum(F.when(F.col("event_type") == "DELIVERY_ATTEMPTED", 1).otherwise(0)).alias("failed_attempts"),
            F.sum(F.when(F.col("event_type") == "DELIVERY_FAILED", 1).otherwise(0)).alias("final_failures"),
            F.sum(F.when(F.col("is_cod") == True, 1).otherwise(0)).alias("cod_deliveries"),
            F.sum("cod_collected").alias("total_cod_collected"),
            F.avg("time_at_location_seconds").alias("avg_time_at_stop_seconds"),
            F.avg("customer_rating").alias("avg_customer_rating"),
            F.count(F.when(F.col("customer_rating").isNotNull(), 1)).alias("ratings_received"),
        )

        # Calculate success rate
        delivery_agg = delivery_agg.withColumn(
            "delivery_success_rate",
            F.when(
                (F.col("successful_deliveries") + F.col("failed_attempts") + F.col("final_failures")) > 0,
                F.col("successful_deliveries") /
                (F.col("successful_deliveries") + F.col("failed_attempts") + F.col("final_failures"))
            ).otherwise(None)
        )

        # Aggregate failure reasons
        failure_reasons = deliveries.filter(
            F.col("failure_reason").isNotNull()
        ).groupBy(
            "agent_id",
            F.to_date("event_timestamp").alias("event_date")
        ).agg(
            F.collect_list("failure_reason").alias("failure_reasons")
        )

        delivery_agg = delivery_agg.join(
            failure_reasons,
            ["agent_id", "event_date"],
            "left"
        )

        return delivery_agg

    def _combine_metrics(
        self,
        shifts: DataFrame,
        distance: DataFrame,
        deliveries: DataFrame
    ) -> DataFrame:
        """Combine all metrics into final shift summary."""
        combined = shifts.join(
            distance.select("agent_id", "event_date", "total_distance_km"),
            ["agent_id", "event_date"],
            "left"
        ).join(
            deliveries,
            ["agent_id", "event_date"],
            "left"
        )

        # Calculate derived metrics
        combined = combined.withColumn(
            "deliveries_per_hour",
            F.when(
                F.col("shift_duration_hours") > 0,
                F.col("successful_deliveries") / F.col("shift_duration_hours")
            ).otherwise(None)
        ).withColumn(
            "km_per_delivery",
            F.when(
                F.col("successful_deliveries") > 0,
                F.col("total_distance_km") / F.col("successful_deliveries")
            ).otherwise(None)
        ).withColumn(
            "utilization_rate",
            F.when(
                F.col("shift_duration_hours") > 0,
                (F.col("successful_deliveries") * F.col("avg_time_at_stop_seconds") / 3600) /
                F.col("shift_duration_hours")
            ).otherwise(None)
        )

        return combined

    def _calculate_rankings(self, shifts: DataFrame) -> DataFrame:
        """Calculate agent rankings within zone."""
        zone_window = Window.partitionBy("zone_id", "event_date").orderBy(
            F.desc("successful_deliveries"),
            F.desc("delivery_success_rate")
        )

        overall_window = Window.partitionBy("event_date").orderBy(
            F.desc("successful_deliveries"),
            F.desc("delivery_success_rate")
        )

        shifts = shifts.withColumn(
            "zone_rank",
            F.row_number().over(zone_window)
        ).withColumn(
            "overall_rank",
            F.row_number().over(overall_window)
        ).withColumn(
            "is_top_performer",
            F.col("zone_rank") <= 3
        )

        return shifts

    def _aggregate_zone_performance(self, shifts: DataFrame) -> DataFrame:
        """Aggregate zone-level performance metrics."""
        zone_agg = shifts.groupBy(
            "zone_id",
            "event_date"
        ).agg(
            F.count("agent_id").alias("active_agents"),
            F.sum("successful_deliveries").alias("total_deliveries"),
            F.sum("final_failures").alias("total_failures"),
            F.avg("delivery_success_rate").alias("avg_success_rate"),
            F.avg("deliveries_per_hour").alias("avg_deliveries_per_hour"),
            F.sum("total_distance_km").alias("total_distance_km"),
            F.sum("total_cod_collected").alias("total_cod_collected"),
            F.avg("avg_customer_rating").alias("avg_customer_rating"),
        )

        # Zone rankings
        date_window = Window.partitionBy("event_date").orderBy(F.desc("total_deliveries"))
        zone_agg = zone_agg.withColumn(
            "zone_performance_rank",
            F.row_number().over(date_window)
        )

        return zone_agg

    def aggregate(self, date: str = None, write_output: bool = True) -> tuple:
        """
        Run the agent shift aggregation pipeline.

        Args:
            date: Process only this date (YYYY-MM-DD format)
            write_output: Whether to write results to Silver layer

        Returns:
            Tuple of (agent_shifts DataFrame, zone_performance DataFrame)
        """
        logger.info(f"Starting agent shift aggregation for date: {date or 'all'}")

        # Read Bronze data
        positions = self._read_agent_positions(date)
        deliveries = self._read_delivery_events(date)

        position_count = positions.count()
        delivery_count = deliveries.count()
        logger.info(f"Read {position_count} positions and {delivery_count} delivery events")

        if position_count == 0:
            logger.warning("No positions found, skipping aggregation")
            return None, None

        # Calculate component metrics
        distance = self._calculate_distance_traveled(positions)
        shifts = self._aggregate_shift_metrics(positions)
        delivery_metrics = self._aggregate_delivery_metrics(deliveries)

        # Combine all metrics
        combined = self._combine_metrics(shifts, distance, delivery_metrics)

        # Add rankings
        with_rankings = self._calculate_rankings(combined)

        # Add metadata
        final_shifts = with_rankings.withColumn(
            "aggregated_at",
            F.current_timestamp()
        ).withColumn(
            "shift_date",
            F.col("event_date")
        )

        # Aggregate zone performance
        zone_performance = self._aggregate_zone_performance(final_shifts)

        shift_count = final_shifts.count()
        logger.info(f"Aggregated {shift_count} agent shifts")

        if write_output:
            # Write agent shifts
            shift_path = f"{self.silver_path}/delivery/agent_shifts"
            (
                final_shifts.write
                .format("delta")
                .mode("append")
                .partitionBy("shift_date")
                .save(shift_path)
            )
            logger.info(f"Wrote agent shifts to {shift_path}")

            # Write zone performance
            zone_path = f"{self.silver_path}/delivery/zone_performance"
            (
                zone_performance.write
                .format("delta")
                .mode("append")
                .partitionBy("event_date")
                .save(zone_path)
            )
            logger.info(f"Wrote zone performance to {zone_path}")

        return final_shifts, zone_performance


def main():
    parser = argparse.ArgumentParser(description="Agent Shift Aggregation Batch Job")
    parser.add_argument("--date", help="Process date (YYYY-MM-DD)")
    parser.add_argument("--bronze-path", default="data/bronze", help="Bronze layer path")
    parser.add_argument("--silver-path", default="data/silver", help="Silver layer path")
    parser.add_argument("--dry-run", action="store_true", help="Don't write output")

    args = parser.parse_args()

    aggregator = AgentShiftAggregator(
        bronze_path=args.bronze_path,
        silver_path=args.silver_path,
    )

    shifts, zones = aggregator.aggregate(
        date=args.date,
        write_output=not args.dry_run
    )

    if shifts:
        print("\n=== Top Performing Agents ===")
        shifts.filter(F.col("is_top_performer")).select(
            "agent_id", "zone_id", "successful_deliveries",
            "delivery_success_rate", "deliveries_per_hour", "zone_rank"
        ).orderBy("zone_id", "zone_rank").show(20)

        print("\n=== Zone Performance ===")
        zones.select(
            "zone_id", "active_agents", "total_deliveries",
            "avg_success_rate", "zone_performance_rank"
        ).orderBy("zone_performance_rank").show()


if __name__ == "__main__":
    main()
