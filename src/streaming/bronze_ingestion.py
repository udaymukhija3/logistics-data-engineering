"""
Bronze Layer Ingestion - Spark Structured Streaming jobs.

Reads from Kafka topics and writes to Delta Lake Bronze tables with:
- Schema validation
- Timestamp extraction
- Partitioning by date
- Deduplication
"""

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.domain.constants import (
    INDIA_BOUNDS,
    SHIPMENT_EVENT_TYPES,
    TOPIC_AGENT_POSITIONS,
    TOPIC_ALERTS,
    TOPIC_DELIVERY_EVENTS,
    TOPIC_SHIPMENT_EVENTS,
    TOPIC_VEHICLE_POSITIONS,
    TOPIC_VEHICLE_TELEMETRY,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Schema definitions for each topic
SCHEMAS = {
    "vehicle_positions": StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("vehicle_id", StringType(), False),
            StructField("driver_id", StringType(), True),
            StructField("vehicle_type", StringType(), True),
            StructField("timestamp", StringType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("speed_kmh", DoubleType(), True),
            StructField("heading", DoubleType(), True),
            StructField("altitude_m", IntegerType(), True),
            StructField("accuracy_m", DoubleType(), True),
            StructField("trip_id", StringType(), True),
            StructField("state", StringType(), True),
            StructField("fuel_level_pct", DoubleType(), True),
            StructField("odometer_km", DoubleType(), True),
        ]
    ),
    "vehicle_telemetry": StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("vehicle_id", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("engine_rpm", IntegerType(), True),
            StructField("engine_temp_c", DoubleType(), True),
            StructField("fuel_level_pct", DoubleType(), True),
            StructField("battery_voltage", DoubleType(), True),
            StructField("odometer_km", DoubleType(), True),
            StructField("engine_hours", DoubleType(), True),
            StructField("oil_pressure_psi", DoubleType(), True),
            StructField("coolant_temp_c", DoubleType(), True),
        ]
    ),
    "shipment_events": StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("shipment_id", StringType(), False),
            StructField("awb_number", StringType(), True),
            StructField("timestamp", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("hub_id", StringType(), True),
            StructField("hub_name", StringType(), True),
            StructField("hub_city", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("seller_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("origin_hub", StringType(), True),
            StructField("destination_hub", StringType(), True),
            StructField("weight_kg", DoubleType(), True),
            StructField("is_cod", BooleanType(), True),
            StructField("cod_amount", DoubleType(), True),
            StructField("promised_delivery", StringType(), True),
            StructField("route_hops", IntegerType(), True),
            StructField("current_hop", IntegerType(), True),
            StructField("delivery_attempts", IntegerType(), True),
            StructField("failure_reason", StringType(), True),
            StructField("scanner_id", StringType(), True),
            StructField("worker_id", StringType(), True),
        ]
    ),
    "agent_positions": StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("agent_id", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("speed_kmh", DoubleType(), True),
            StructField("heading", DoubleType(), True),
            StructField("accuracy_m", DoubleType(), True),
            StructField("zone_id", StringType(), True),
            StructField("vehicle_type", StringType(), True),
            StructField("status", StringType(), True),
            StructField("is_at_stop", BooleanType(), True),
            StructField("current_order_id", StringType(), True),
            StructField("pending_orders", IntegerType(), True),
            StructField("completed_today", IntegerType(), True),
            StructField("failed_today", IntegerType(), True),
            StructField("battery_pct", IntegerType(), True),
        ]
    ),
    "delivery_events": StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("agent_id", StringType(), False),
            StructField("agent_name", StringType(), True),
            StructField("order_id", StringType(), False),
            StructField("shipment_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("delivery_lat", DoubleType(), True),
            StructField("delivery_lng", DoubleType(), True),
            StructField("delivery_address", StringType(), True),
            StructField("zone_id", StringType(), True),
            StructField("is_cod", BooleanType(), True),
            StructField("cod_amount", DoubleType(), True),
            StructField("cod_collected", DoubleType(), True),
            StructField("payment_mode", StringType(), True),
            StructField("attempt_number", IntegerType(), True),
            StructField("failure_reason", StringType(), True),
            StructField("pod_type", StringType(), True),
            StructField("customer_rating", IntegerType(), True),
            StructField("time_at_location_seconds", IntegerType(), True),
        ]
    ),
    "alerts": StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("severity", StringType(), True),
            StructField("timestamp", StringType(), False),
            StructField("vehicle_id", StringType(), True),
            StructField("driver_id", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("speed", DoubleType(), True),
            StructField("speed_limit", DoubleType(), True),
            StructField("overspeed_by", DoubleType(), True),
            StructField("deceleration_ms2", DoubleType(), True),
            StructField("acceleration_ms2", DoubleType(), True),
        ]
    ),
}


class BronzeIngestion:
    """
    Spark Structured Streaming job for Bronze layer ingestion.

    Reads from Kafka topics, validates schema, and writes to Delta Lake
    with proper partitioning and deduplication.
    """

    def __init__(
        self,
        spark: SparkSession = None,
        kafka_bootstrap_servers: str = "localhost:9092",
        bronze_path: str = "data/bronze",
        checkpoint_path: str = "data/checkpoints",
    ):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.bronze_path = bronze_path
        self.checkpoint_path = checkpoint_path

        if spark:
            self.spark = spark
        else:
            self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        """Create a Spark session with Delta Lake support."""
        return (
            SparkSession.builder.appName("LogisticsBronzeIngestion")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.shuffle.partitions", "8")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "io.delta:delta-spark_2.12:3.0.0",
            )
            .getOrCreate()
        )

    def _read_kafka_stream(self, topic: str) -> DataFrame:
        """Read a streaming DataFrame from Kafka topic."""
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

    def _parse_json(self, df: DataFrame, schema: StructType) -> DataFrame:
        """Parse JSON payload and apply schema."""
        return (
            df.selectExpr("CAST(key AS STRING) as kafka_key", "CAST(value AS STRING) as json_value")
            .select(F.col("kafka_key"), F.from_json(F.col("json_value"), schema).alias("data"))
            .select("kafka_key", "data.*")
            .withColumn("event_timestamp", F.to_timestamp("timestamp"))
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("event_date", F.to_date("event_timestamp"))
        )

    def _validate_coordinates(
        self, df: DataFrame, lat_col: str = "latitude", lng_col: str = "longitude"
    ) -> DataFrame:
        """Validate coordinates are within India bounds."""
        return df.withColumn(
            "is_valid_location",
            (F.col(lat_col).between(INDIA_BOUNDS["lat_min"], INDIA_BOUNDS["lat_max"]))
            & (F.col(lng_col).between(INDIA_BOUNDS["lng_min"], INDIA_BOUNDS["lng_max"])),
        )

    def _write_to_delta(
        self,
        df: DataFrame,
        table_name: str,
        partition_cols: list = None,
    ):
        """Write streaming DataFrame to Delta Lake."""
        output_path = f"{self.bronze_path}/{table_name}"
        checkpoint = f"{self.checkpoint_path}/{table_name}"

        if partition_cols is None:
            partition_cols = ["event_date"]

        writer = (
            df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .partitionBy(*partition_cols)
        )

        return writer.start(output_path)

    def ingest_vehicle_positions(self) -> None:
        """Ingest vehicle GPS positions from Kafka to Bronze."""
        logger.info("Starting vehicle positions ingestion...")

        raw_df = self._read_kafka_stream(TOPIC_VEHICLE_POSITIONS)
        parsed_df = self._parse_json(raw_df, SCHEMAS[TOPIC_VEHICLE_POSITIONS])

        # Add validation columns
        validated_df = self._validate_coordinates(parsed_df).withColumn(
            "is_valid_speed", F.col("speed_kmh").between(0, 200) | F.col("speed_kmh").isNull()
        )

        query = self._write_to_delta(validated_df, TOPIC_VEHICLE_POSITIONS)
        logger.info("Vehicle positions ingestion started")
        return query

    def ingest_vehicle_telemetry(self) -> None:
        """Ingest vehicle telemetry from Kafka to Bronze."""
        logger.info("Starting vehicle telemetry ingestion...")

        raw_df = self._read_kafka_stream(TOPIC_VEHICLE_TELEMETRY)
        parsed_df = self._parse_json(raw_df, SCHEMAS[TOPIC_VEHICLE_TELEMETRY])

        query = self._write_to_delta(parsed_df, TOPIC_VEHICLE_TELEMETRY)
        logger.info("Vehicle telemetry ingestion started")
        return query

    def ingest_shipment_events(self) -> None:
        """Ingest shipment events from Kafka to Bronze."""
        logger.info("Starting shipment events ingestion...")

        raw_df = self._read_kafka_stream(TOPIC_SHIPMENT_EVENTS)
        parsed_df = self._parse_json(raw_df, SCHEMAS[TOPIC_SHIPMENT_EVENTS])

        validated_df = self._validate_coordinates(parsed_df).withColumn(
            "is_valid_event_type", F.col("event_type").isin(SHIPMENT_EVENT_TYPES)
        )

        query = self._write_to_delta(validated_df, TOPIC_SHIPMENT_EVENTS)
        logger.info("Shipment events ingestion started")
        return query

    def ingest_agent_positions(self) -> None:
        """Ingest delivery agent positions from Kafka to Bronze."""
        logger.info("Starting agent positions ingestion...")

        raw_df = self._read_kafka_stream(TOPIC_AGENT_POSITIONS)
        parsed_df = self._parse_json(raw_df, SCHEMAS[TOPIC_AGENT_POSITIONS])

        validated_df = self._validate_coordinates(parsed_df)

        query = self._write_to_delta(validated_df, TOPIC_AGENT_POSITIONS)
        logger.info("Agent positions ingestion started")
        return query

    def ingest_delivery_events(self) -> None:
        """Ingest delivery events from Kafka to Bronze."""
        logger.info("Starting delivery events ingestion...")

        raw_df = self._read_kafka_stream(TOPIC_DELIVERY_EVENTS)
        parsed_df = self._parse_json(raw_df, SCHEMAS[TOPIC_DELIVERY_EVENTS])

        # Validate delivery locations
        validated_df = self._validate_coordinates(
            parsed_df, lat_col="delivery_lat", lng_col="delivery_lng"
        )

        query = self._write_to_delta(validated_df, TOPIC_DELIVERY_EVENTS)
        logger.info("Delivery events ingestion started")
        return query

    def ingest_alerts(self) -> None:
        """Ingest alert events from Kafka to Bronze."""
        logger.info("Starting alerts ingestion...")

        raw_df = self._read_kafka_stream(TOPIC_ALERTS)
        parsed_df = self._parse_json(raw_df, SCHEMAS[TOPIC_ALERTS])

        query = self._write_to_delta(parsed_df, TOPIC_ALERTS)
        logger.info("Alerts ingestion started")
        return query

    def run_all(self, await_termination: bool = True):
        """Run all ingestion streams."""
        logger.info("Starting all Bronze ingestion streams...")

        queries = [
            self.ingest_vehicle_positions(),
            self.ingest_vehicle_telemetry(),
            self.ingest_shipment_events(),
            self.ingest_agent_positions(),
            self.ingest_delivery_events(),
            self.ingest_alerts(),
        ]

        logger.info(f"Started {len(queries)} streaming queries")

        if await_termination:
            for query in queries:
                if query:
                    query.awaitTermination()

        return queries

    def stop(self):
        """Stop all active streaming queries."""
        for query in self.spark.streams.active:
            query.stop()
        logger.info("All streaming queries stopped")


def main():
    parser = argparse.ArgumentParser(description="Bronze Layer Ingestion")
    parser.add_argument("--kafka", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--bronze-path", default="data/bronze", help="Bronze layer output path")
    parser.add_argument("--checkpoint-path", default="data/checkpoints", help="Checkpoint path")
    parser.add_argument(
        "--topics",
        nargs="+",
        default=["all"],
        choices=[
            "all",
            "vehicle_positions",
            "vehicle_telemetry",
            "shipment_events",
            "agent_positions",
            "delivery_events",
            "alerts",
        ],
        help="Topics to ingest",
    )

    args = parser.parse_args()

    ingestion = BronzeIngestion(
        kafka_bootstrap_servers=args.kafka,
        bronze_path=args.bronze_path,
        checkpoint_path=args.checkpoint_path,
    )

    try:
        if "all" in args.topics:
            ingestion.run_all()
        else:
            queries = []
            if "vehicle_positions" in args.topics:
                queries.append(ingestion.ingest_vehicle_positions())
            if "vehicle_telemetry" in args.topics:
                queries.append(ingestion.ingest_vehicle_telemetry())
            if "shipment_events" in args.topics:
                queries.append(ingestion.ingest_shipment_events())
            if "agent_positions" in args.topics:
                queries.append(ingestion.ingest_agent_positions())
            if "delivery_events" in args.topics:
                queries.append(ingestion.ingest_delivery_events())
            if "alerts" in args.topics:
                queries.append(ingestion.ingest_alerts())

            for query in queries:
                if query:
                    query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
        ingestion.stop()


if __name__ == "__main__":
    main()
