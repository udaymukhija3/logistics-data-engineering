#!/bin/bash
# infrastructure/kafka/topics.sh
# Create Kafka topics for Unified Logistics Platform

KAFKA_BOOTSTRAP="localhost:9092"

echo "Creating Kafka topics..."

# Fleet Telematics topics
kafka-topics.sh --create --if-not-exists \
    --bootstrap-server $KAFKA_BOOTSTRAP \
    --topic vehicle_positions \
    --partitions 6 \
    --replication-factor 1 \
    --config retention.ms=86400000 \
    --config cleanup.policy=delete

kafka-topics.sh --create --if-not-exists \
    --bootstrap-server $KAFKA_BOOTSTRAP \
    --topic vehicle_telemetry \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=86400000

# Shipment Tracking topics
kafka-topics.sh --create --if-not-exists \
    --bootstrap-server $KAFKA_BOOTSTRAP \
    --topic shipment_events \
    --partitions 6 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --config cleanup.policy=delete

# Last-Mile Delivery topics
kafka-topics.sh --create --if-not-exists \
    --bootstrap-server $KAFKA_BOOTSTRAP \
    --topic agent_positions \
    --partitions 6 \
    --replication-factor 1 \
    --config retention.ms=86400000

kafka-topics.sh --create --if-not-exists \
    --bootstrap-server $KAFKA_BOOTSTRAP \
    --topic delivery_events \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000

# Alerts topic (all modules)
kafka-topics.sh --create --if-not-exists \
    --bootstrap-server $KAFKA_BOOTSTRAP \
    --topic alerts \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000

echo "Listing all topics:"
kafka-topics.sh --list --bootstrap-server $KAFKA_BOOTSTRAP

echo "Done!"
