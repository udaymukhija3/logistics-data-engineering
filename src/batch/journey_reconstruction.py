"""
Journey Reconstruction - Batch job to reconstruct shipment journeys from scan events.

This job:
1. Reads shipment events from Bronze layer
2. Reconstructs the complete journey for each shipment
3. Calculates hub dwell times and transit times
4. Identifies SLA breaches and stuck shipments
5. Writes journey data to Silver layer
"""

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class JourneyReconstructor:
    """
    Reconstructs shipment journeys from scan events.

    Creates:
    - Journey summary (start to end)
    - Hub-by-hub breakdown with dwell times
    - SLA tracking and breach detection
    - Bottleneck identification
    """

    def __init__(
        self,
        spark: SparkSession = None,
        bronze_path: str = "data/bronze",
        silver_path: str = "data/silver",
        stuck_threshold_hours: int = 24,
        bottleneck_dwell_hours: int = 12,
    ):
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.stuck_threshold_hours = stuck_threshold_hours
        self.bottleneck_dwell_hours = bottleneck_dwell_hours

        if spark:
            self.spark = spark
        else:
            self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Delta Lake support."""
        return (
            SparkSession.builder.appName("JourneyReconstruction")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate()
        )

    def _read_bronze_events(self, date: str = None) -> DataFrame:
        """Read shipment events from Bronze layer."""
        path = f"{self.bronze_path}/shipment_events"

        df = self.spark.read.format("delta").load(path)

        if date:
            df = df.filter(F.col("event_date") == date)

        return df

    def _sequence_events(self, df: DataFrame) -> DataFrame:
        """Order and sequence events for each shipment."""
        shipment_window = Window.partitionBy("shipment_id").orderBy("event_timestamp")

        df = (
            df.withColumn("event_sequence", F.row_number().over(shipment_window))
            .withColumn("prev_event_type", F.lag("event_type").over(shipment_window))
            .withColumn("prev_event_time", F.lag("event_timestamp").over(shipment_window))
            .withColumn("prev_hub_id", F.lag("hub_id").over(shipment_window))
            .withColumn("next_event_type", F.lead("event_type").over(shipment_window))
            .withColumn("next_event_time", F.lead("event_timestamp").over(shipment_window))
        )

        # Calculate time since previous event
        df = df.withColumn(
            "time_since_prev_hours",
            (F.unix_timestamp("event_timestamp") - F.unix_timestamp("prev_event_time")) / 3600,
        )

        return df

    def _identify_journey_stages(self, df: DataFrame) -> DataFrame:
        """Categorize each event into journey stage."""
        df = df.withColumn(
            "journey_stage",
            F.when(
                F.col("event_type").isin(["CREATED", "PICKUP_SCHEDULED", "PICKED_UP"]), "FIRST_MILE"
            )
            .when(
                F.col("event_type").isin(
                    [
                        "HUB_ARRIVED",
                        "HUB_INSCAN",
                        "HUB_SORTED",
                        "HUB_OUTSCAN",
                        "HUB_DEPARTED",
                        "IN_TRANSIT",
                    ]
                ),
                "MID_MILE",
            )
            .when(
                F.col("event_type").isin(
                    ["OUT_FOR_DELIVERY", "DELIVERY_ATTEMPTED", "DELIVERED", "DELIVERY_FAILED"]
                ),
                "LAST_MILE",
            )
            .otherwise("OTHER"),
        )

        return df

    def _calculate_hub_dwell_times(self, df: DataFrame) -> DataFrame:
        """Calculate dwell time at each hub."""
        # Filter to hub events only
        hub_events = df.filter(F.col("event_type").like("HUB_%"))

        # Window for hub processing
        hub_window = Window.partitionBy("shipment_id", "hub_id").orderBy("event_timestamp")

        hub_events = hub_events.withColumn("hub_event_seq", F.row_number().over(hub_window))

        # Get hub arrival and departure times
        hub_summary = hub_events.groupBy("shipment_id", "hub_id", "hub_name", "hub_city").agg(
            F.min("event_timestamp").alias("hub_arrival_time"),
            F.max("event_timestamp").alias("hub_departure_time"),
            F.count("*").alias("hub_event_count"),
            F.collect_list("event_type").alias("hub_events"),
        )

        # Calculate dwell time
        hub_summary = hub_summary.withColumn(
            "hub_dwell_hours",
            (F.unix_timestamp("hub_departure_time") - F.unix_timestamp("hub_arrival_time")) / 3600,
        ).withColumn("is_bottleneck", F.col("hub_dwell_hours") > self.bottleneck_dwell_hours)

        return hub_summary

    def _reconstruct_journeys(self, df: DataFrame) -> DataFrame:
        """Create journey summary for each shipment."""
        journeys = df.groupBy(
            "shipment_id",
            "awb_number",
            "seller_id",
            "customer_id",
            "origin_hub",
            "destination_hub",
            "is_cod",
            "cod_amount",
            "promised_delivery",
            "route_hops",
        ).agg(
            F.min("event_timestamp").alias("journey_start_time"),
            F.max("event_timestamp").alias("journey_end_time"),
            F.first("event_type").alias("first_event"),
            F.last("event_type").alias("last_event"),
            F.count("*").alias("total_events"),
            F.countDistinct("hub_id").alias("hubs_visited"),
            F.max("delivery_attempts").alias("delivery_attempts"),
            F.last("failure_reason").alias("last_failure_reason"),
            F.sum(
                F.when(F.col("time_since_prev_hours") > self.stuck_threshold_hours, 1).otherwise(0)
            ).alias("stuck_incidents"),
            F.max("time_since_prev_hours").alias("max_gap_hours"),
        )

        return journeys

    def _calculate_sla_status(self, journeys: DataFrame) -> DataFrame:
        """Calculate SLA status for each journey."""
        journeys = (
            journeys.withColumn("promised_delivery_ts", F.to_timestamp("promised_delivery"))
            .withColumn(
                "journey_duration_hours",
                (F.unix_timestamp("journey_end_time") - F.unix_timestamp("journey_start_time"))
                / 3600,
            )
            .withColumn("journey_duration_days", F.col("journey_duration_hours") / 24)
        )

        # Determine SLA status
        journeys = journeys.withColumn(
            "sla_status",
            F.when(
                F.col("last_event") == "DELIVERED",
                F.when(F.col("journey_end_time") <= F.col("promised_delivery_ts"), "MET").otherwise(
                    "BREACHED"
                ),
            )
            .when(F.col("last_event").isin(["DELIVERY_FAILED", "RETURNED_TO_ORIGIN"]), "FAILED")
            .otherwise("IN_PROGRESS"),
        ).withColumn(
            "sla_variance_hours",
            F.when(
                F.col("last_event") == "DELIVERED",
                (F.unix_timestamp("journey_end_time") - F.unix_timestamp("promised_delivery_ts"))
                / 3600,
            ).otherwise(None),
        )

        return journeys

    def _classify_journeys(self, journeys: DataFrame) -> DataFrame:
        """Classify journeys by outcome and characteristics."""
        journeys = (
            journeys.withColumn(
                "journey_outcome",
                F.when(F.col("last_event") == "DELIVERED", "DELIVERED")
                .when(F.col("last_event") == "DELIVERY_FAILED", "FAILED")
                .when(F.col("last_event") == "RETURNED_TO_ORIGIN", "RETURNED")
                .when(F.col("stuck_incidents") > 0, "STUCK")
                .otherwise("IN_TRANSIT"),
            )
            .withColumn("had_delivery_issues", F.col("delivery_attempts") > 1)
            .withColumn("is_express", F.col("route_hops") <= 2)
            .withColumn(
                "complexity",
                F.when(F.col("hubs_visited") <= 2, "SIMPLE")
                .when(F.col("hubs_visited") <= 4, "MODERATE")
                .otherwise("COMPLEX"),
            )
        )

        return journeys

    def reconstruct(self, date: str = None, write_output: bool = True) -> tuple:
        """
        Run the journey reconstruction pipeline.

        Args:
            date: Process only this date (YYYY-MM-DD format)
            write_output: Whether to write results to Silver layer

        Returns:
            Tuple of (journeys DataFrame, hub_dwell DataFrame)
        """
        logger.info(f"Starting journey reconstruction for date: {date or 'all'}")

        # Read Bronze data
        events = self._read_bronze_events(date)

        if events.limit(1).count() == 0:
            logger.warning("No events found, skipping reconstruction")
            return None, None

        # Sequence events
        sequenced = self._sequence_events(events)

        # Identify journey stages
        with_stages = self._identify_journey_stages(sequenced)

        # Calculate hub dwell times
        hub_dwell = self._calculate_hub_dwell_times(with_stages)

        # Reconstruct journeys
        journeys = self._reconstruct_journeys(with_stages)

        # Calculate SLA status
        with_sla = self._calculate_sla_status(journeys)

        # Classify journeys
        classified = self._classify_journeys(with_sla)

        # Add metadata
        final_journeys = classified.withColumn(
            "reconstructed_at", F.current_timestamp()
        ).withColumn("journey_date", F.to_date("journey_start_time"))
        final_journeys.persist(StorageLevel.MEMORY_AND_DISK)

        journey_count = final_journeys.count()
        logger.info(f"Reconstructed {journey_count} journeys")

        if write_output:
            # Write journeys
            journey_path = f"{self.silver_path}/shipment/journeys"
            (
                final_journeys.write.format("delta")
                .mode("append")
                .partitionBy("journey_date")
                .save(journey_path)
            )
            logger.info(f"Wrote journeys to {journey_path}")

            # Write hub dwell times
            hub_path = f"{self.silver_path}/shipment/hub_dwell"
            (hub_dwell.write.format("delta").mode("append").save(hub_path))
            logger.info(f"Wrote hub dwell times to {hub_path}")

        final_journeys.unpersist()

        return final_journeys, hub_dwell

    def get_stuck_shipments(self, hours_threshold: int = None) -> DataFrame:
        """Identify shipments that appear stuck."""
        threshold = hours_threshold or self.stuck_threshold_hours

        events = self._read_bronze_events()
        sequenced = self._sequence_events(events)

        # Find shipments with long gaps that aren't delivered
        stuck = (
            sequenced.filter(
                (F.col("time_since_prev_hours") > threshold)
                & (
                    ~F.col("event_type").isin(
                        ["DELIVERED", "DELIVERY_FAILED", "RETURNED_TO_ORIGIN"]
                    )
                )
            )
            .select(
                "shipment_id",
                "awb_number",
                "hub_id",
                "hub_name",
                "event_type",
                "event_timestamp",
                "time_since_prev_hours",
            )
            .orderBy(F.desc("time_since_prev_hours"))
        )

        return stuck


def main():
    parser = argparse.ArgumentParser(description="Journey Reconstruction Batch Job")
    parser.add_argument("--date", help="Process date (YYYY-MM-DD)")
    parser.add_argument("--bronze-path", default="data/bronze", help="Bronze layer path")
    parser.add_argument("--silver-path", default="data/silver", help="Silver layer path")
    parser.add_argument("--stuck-threshold", type=int, default=24, help="Stuck threshold in hours")
    parser.add_argument("--dry-run", action="store_true", help="Don't write output")

    args = parser.parse_args()

    reconstructor = JourneyReconstructor(
        bronze_path=args.bronze_path,
        silver_path=args.silver_path,
        stuck_threshold_hours=args.stuck_threshold,
    )

    journeys, hub_dwell = reconstructor.reconstruct(date=args.date, write_output=not args.dry_run)

    if journeys:
        print("\n=== Journey Summary ===")
        journeys.groupBy("journey_outcome").count().show()

        print("\n=== SLA Status ===")
        journeys.groupBy("sla_status").count().show()

        print("\n=== Sample Journeys ===")
        journeys.select(
            "shipment_id",
            "origin_hub",
            "destination_hub",
            "journey_duration_hours",
            "sla_status",
            "journey_outcome",
        ).show(10)


if __name__ == "__main__":
    main()
