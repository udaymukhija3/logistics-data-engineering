"""
Base simulator class with common functionality.
"""

import json
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Optional
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseSimulator(ABC):
    """Base class for all data simulators."""

    def __init__(
        self,
        kafka_bootstrap_servers: str = "localhost:9092",
        topic: str = None,
        batch_size: int = 100,
        flush_interval_seconds: float = 1.0,
    ):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.topic = topic
        self.batch_size = batch_size
        self.flush_interval_seconds = flush_interval_seconds
        self.producer: Optional[KafkaProducer] = None
        self.message_count = 0
        self.start_time = None

    def connect(self, max_retries: int = 5, retry_delay: float = 2.0) -> bool:
        """Connect to Kafka with retry logic."""
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    batch_size=16384,
                    linger_ms=10,
                )
                logger.info(f"Connected to Kafka at {self.kafka_bootstrap_servers}")
                return True
            except NoBrokersAvailable:
                logger.warning(f"Kafka not available, attempt {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)

        logger.error("Failed to connect to Kafka")
        return False

    def send(self, message: Dict[str, Any], key: str = None) -> bool:
        """Send a message to Kafka."""
        if not self.producer:
            logger.error("Producer not connected")
            return False

        try:
            self.producer.send(
                self.topic,
                key=key,
                value=message,
            )
            self.message_count += 1

            if self.message_count % self.batch_size == 0:
                self.producer.flush()

            return True
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            return False

    def close(self):
        """Close the Kafka producer."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info(f"Producer closed. Total messages sent: {self.message_count}")

    @abstractmethod
    def generate_event(self) -> Dict[str, Any]:
        """Generate a single event. Must be implemented by subclasses."""
        pass

    @abstractmethod
    def run(self, duration_seconds: int = None, max_events: int = None):
        """Run the simulator. Must be implemented by subclasses."""
        pass

    def _get_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        return datetime.utcnow().isoformat() + "Z"

    def _log_stats(self):
        """Log simulator statistics."""
        if self.start_time:
            elapsed = time.time() - self.start_time
            rate = self.message_count / elapsed if elapsed > 0 else 0
            logger.info(f"Messages: {self.message_count}, Rate: {rate:.1f}/sec")
