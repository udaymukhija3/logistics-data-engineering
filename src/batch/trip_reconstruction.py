"""
Trip Reconstruction - Batch job to reconstruct vehicle trips from GPS points.

This job:
1. Reads raw GPS positions from Bronze layer
2. Identifies trip boundaries (gaps > 30 minutes = new trip)
3. Calculates trip metrics (distance, duration, avg speed)
4. Writes reconstructed trips to Silver layer
"""

import argparse
import logging
import math
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, LongType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TripReconstructor:
    """
    Reconstructs vehicle trips from raw GPS position data.

    Algorithm:
    1. Order GPS points by vehicle and timestamp
    2. Calculate time gap between consecutive points
    3. If gap > threshold (30 min), start a new trip
    4. Aggregate trip statistics (distance, duration, speeds)
    5. Filter out invalid/too-short trips
    """

    def __init__(
        self,
        spark: SparkSession = None,
        bronze_path: str = "data/bronze",
        silver_path: str = "data/silver",
        trip_gap_minutes: int = 30,
        min_trip_duration_minutes: int = 5,
        min_trip_positions: int = 5,
    ):
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.trip_gap_minutes = trip_gap_minutes
        self.min_trip_duration_minutes = min_trip_duration_minutes
        self.min_trip_positions = min_trip_positions

        if spark:
            self.spark = spark
        else:
            self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Delta Lake support."""
        return (
            SparkSession.builder
            .appName("TripReconstruction")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate()
        )

    def _read_bronze_positions(self, date: str = None) -> DataFrame:
        """Read vehicle positions from Bronze layer."""
        path = f"{self.bronze_path}/vehicle_positions"

        df = self.spark.read.format("delta").load(path)

        if date:
            df = df.filter(F.col("event_date") == date)

        # Filter valid positions only
        df = df.filter(
            F.col("is_valid_location") == True
        ).filter(
            F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
        )

        return df

    def _calculate_distance_udf(self):
        """Create UDF for haversine distance calculation."""
        @F.udf(DoubleType())
        def haversine(lat1, lng1, lat2, lng2):
            if any(v is None for v in [lat1, lng1, lat2, lng2]):
                return None

            R = 6371  # Earth's radius in km

            lat1_rad = math.radians(lat1)
            lat2_rad = math.radians(lat2)
            delta_lat = math.radians(lat2 - lat1)
            delta_lng = math.radians(lng2 - lng1)

            a = (math.sin(delta_lat/2)**2 +
                 math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lng/2)**2)
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

            return R * c

        return haversine

    def _identify_trip_boundaries(self, df: DataFrame) -> DataFrame:
        """
        Identify trip boundaries based on time gaps.

        A new trip starts when:
        - Gap since last position > threshold (default 30 min)
        - Vehicle state changes from IDLE/STOPPED to MOVING
        """
        # Window for ordering by vehicle and time
        vehicle_window = Window.partitionBy("vehicle_id").orderBy("event_timestamp")

        # Calculate time since previous position
        df = df.withColumn(
            "prev_timestamp",
            F.lag("event_timestamp").over(vehicle_window)
        ).withColumn(
            "time_gap_minutes",
            (F.unix_timestamp("event_timestamp") - F.unix_timestamp("prev_timestamp")) / 60
        )

        # Get previous state
        df = df.withColumn(
            "prev_state",
            F.lag("state").over(vehicle_window)
        )

        # Mark trip boundaries
        df = df.withColumn(
            "is_trip_start",
            (F.col("time_gap_minutes") > self.trip_gap_minutes) |
            F.col("prev_timestamp").isNull() |
            (
                (F.col("state") == "MOVING") &
                (F.col("prev_state").isin(["IDLE", "STOPPED"]))
            )
        )

        # Assign trip IDs using cumulative sum of trip starts
        df = df.withColumn(
            "trip_sequence",
            F.sum(F.when(F.col("is_trip_start"), 1).otherwise(0)).over(vehicle_window)
        ).withColumn(
            "reconstructed_trip_id",
            F.concat(
                F.col("vehicle_id"),
                F.lit("_TRIP_"),
                F.date_format("event_timestamp", "yyyyMMdd"),
                F.lit("_"),
                F.lpad(F.col("trip_sequence").cast("string"), 3, "0")
            )
        )

        return df

    def _calculate_segment_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate distance and duration for each GPS segment."""
        vehicle_window = Window.partitionBy("vehicle_id").orderBy("event_timestamp")
        haversine = self._calculate_distance_udf()

        # Get previous position
        df = df.withColumn(
            "prev_lat",
            F.lag("latitude").over(vehicle_window)
        ).withColumn(
            "prev_lng",
            F.lag("longitude").over(vehicle_window)
        )

        # Calculate segment distance
        df = df.withColumn(
            "segment_distance_km",
            F.when(
                F.col("is_trip_start"),
                F.lit(0.0)
            ).otherwise(
                haversine(
                    F.col("prev_lat"), F.col("prev_lng"),
                    F.col("latitude"), F.col("longitude")
                )
            )
        )

        # Calculate segment duration in seconds
        df = df.withColumn(
            "segment_duration_seconds",
            F.when(
                F.col("is_trip_start"),
                F.lit(0)
            ).otherwise(
                F.unix_timestamp("event_timestamp") - F.unix_timestamp("prev_timestamp")
            )
        )

        return df

    def _aggregate_trips(self, df: DataFrame) -> DataFrame:
        """Aggregate GPS points into trip summaries."""
        trips = df.groupBy(
            "reconstructed_trip_id",
            "vehicle_id",
            "driver_id",
            "vehicle_type"
        ).agg(
            F.min("event_timestamp").alias("trip_start_time"),
            F.max("event_timestamp").alias("trip_end_time"),
            F.first("latitude").alias("start_latitude"),
            F.first("longitude").alias("start_longitude"),
            F.last("latitude").alias("end_latitude"),
            F.last("longitude").alias("end_longitude"),
            F.sum("segment_distance_km").alias("total_distance_km"),
            F.count("*").alias("position_count"),
            F.avg("speed_kmh").alias("avg_speed_kmh"),
            F.max("speed_kmh").alias("max_speed_kmh"),
            F.min("fuel_level_pct").alias("min_fuel_pct"),
            F.max("fuel_level_pct").alias("max_fuel_pct"),
            F.sum(
                F.when(F.col("state") == "STOPPED", F.col("segment_duration_seconds")).otherwise(0)
            ).alias("total_stop_time_seconds"),
            F.first("trip_id").alias("original_trip_id"),  # From simulator if present
        )

        # Calculate derived metrics
        trips = trips.withColumn(
            "trip_duration_minutes",
            (F.unix_timestamp("trip_end_time") - F.unix_timestamp("trip_start_time")) / 60
        ).withColumn(
            "moving_time_minutes",
            F.col("trip_duration_minutes") - (F.col("total_stop_time_seconds") / 60)
        ).withColumn(
            "fuel_consumed_pct",
            F.col("max_fuel_pct") - F.col("min_fuel_pct")
        ).withColumn(
            "fuel_efficiency_km_per_pct",
            F.when(
                F.col("fuel_consumed_pct") > 0,
                F.col("total_distance_km") / F.col("fuel_consumed_pct")
            ).otherwise(None)
        )

        return trips

    def _filter_valid_trips(self, trips: DataFrame) -> DataFrame:
        """Filter out invalid or too-short trips."""
        return trips.filter(
            (F.col("trip_duration_minutes") >= self.min_trip_duration_minutes) &
            (F.col("position_count") >= self.min_trip_positions) &
            (F.col("total_distance_km") > 0.1)  # At least 100m
        )

    def _add_trip_classification(self, trips: DataFrame) -> DataFrame:
        """Classify trips by type and characteristics."""
        haversine = self._calculate_distance_udf()

        trips = trips.withColumn(
            "straight_line_distance_km",
            haversine(
                F.col("start_latitude"), F.col("start_longitude"),
                F.col("end_latitude"), F.col("end_longitude")
            )
        ).withColumn(
            "route_efficiency",
            F.when(
                F.col("total_distance_km") > 0,
                F.col("straight_line_distance_km") / F.col("total_distance_km")
            ).otherwise(None)
        ).withColumn(
            "trip_type",
            F.when(F.col("straight_line_distance_km") < 1, "LOCAL")
            .when(F.col("straight_line_distance_km") < 50, "INTERCITY_SHORT")
            .when(F.col("straight_line_distance_km") < 200, "INTERCITY_MEDIUM")
            .otherwise("INTERCITY_LONG")
        ).withColumn(
            "is_round_trip",
            F.col("straight_line_distance_km") < 2  # Ends within 2km of start
        )

        return trips

    def reconstruct(self, date: str = None, write_output: bool = True) -> DataFrame:
        """
        Run the trip reconstruction pipeline.

        Args:
            date: Process only this date (YYYY-MM-DD format)
            write_output: Whether to write results to Silver layer

        Returns:
            DataFrame of reconstructed trips
        """
        logger.info(f"Starting trip reconstruction for date: {date or 'all'}")

        # Read Bronze data
        positions = self._read_bronze_positions(date)
        logger.info(f"Read {positions.count()} GPS positions")

        # Identify trip boundaries
        with_boundaries = self._identify_trip_boundaries(positions)

        # Calculate segment metrics
        with_metrics = self._calculate_segment_metrics(with_boundaries)

        # Aggregate into trips
        trips = self._aggregate_trips(with_metrics)

        # Filter valid trips
        valid_trips = self._filter_valid_trips(trips)

        # Add classifications
        classified_trips = self._add_trip_classification(valid_trips)

        # Add metadata
        final_trips = classified_trips.withColumn(
            "reconstructed_at",
            F.current_timestamp()
        ).withColumn(
            "trip_date",
            F.to_date("trip_start_time")
        )

        trip_count = final_trips.count()
        logger.info(f"Reconstructed {trip_count} valid trips")

        if write_output and trip_count > 0:
            output_path = f"{self.silver_path}/fleet/trips"
            (
                final_trips.write
                .format("delta")
                .mode("append")
                .partitionBy("trip_date")
                .save(output_path)
            )
            logger.info(f"Wrote trips to {output_path}")

        return final_trips


def main():
    parser = argparse.ArgumentParser(description="Trip Reconstruction Batch Job")
    parser.add_argument("--date", help="Process date (YYYY-MM-DD)")
    parser.add_argument("--bronze-path", default="data/bronze", help="Bronze layer path")
    parser.add_argument("--silver-path", default="data/silver", help="Silver layer path")
    parser.add_argument("--trip-gap", type=int, default=30, help="Trip gap threshold in minutes")
    parser.add_argument("--min-duration", type=int, default=5, help="Minimum trip duration in minutes")
    parser.add_argument("--dry-run", action="store_true", help="Don't write output")

    args = parser.parse_args()

    reconstructor = TripReconstructor(
        bronze_path=args.bronze_path,
        silver_path=args.silver_path,
        trip_gap_minutes=args.trip_gap,
        min_trip_duration_minutes=args.min_duration,
    )

    trips = reconstructor.reconstruct(
        date=args.date,
        write_output=not args.dry_run
    )

    # Show sample
    trips.show(10, truncate=False)


if __name__ == "__main__":
    main()
