#!/usr/bin/env python3
"""
Run all simulators concurrently.
"""

import argparse
import logging
import signal
import sys
import threading
from typing import List

from src.utils.validation import require_positive_int, require_positive_number

from .delivery_simulator import DeliverySimulator
from .shipment_simulator import ShipmentSimulator
from .vehicle_simulator import VehicleSimulator

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SimulatorOrchestrator:
    """Orchestrates all simulators running concurrently."""

    def __init__(
        self,
        kafka_bootstrap_servers: str = "localhost:9092",
        num_vehicles: int = 50,
        num_agents: int = 100,
        shipments_per_minute: float = 10,
    ):
        require_positive_int(num_vehicles, "num_vehicles")
        require_positive_int(num_agents, "num_agents")
        require_positive_number(shipments_per_minute, "shipments_per_minute")

        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.num_vehicles = num_vehicles
        self.num_agents = num_agents
        self.shipments_per_minute = shipments_per_minute
        self.threads: List[threading.Thread] = []
        self.running = False
        self.simulators = []
        self.errors: List[str] = []

    def _run_with_guard(self, runner, name: str, duration: int = None, max_events: int = None):
        """Run a simulator function with exception capture."""
        try:
            runner(duration=duration, max_events=max_events)
        except Exception:
            logger.exception("%s failed", name)
            self.errors.append(name)
            # Fail fast to avoid orphaned simulator threads when one module crashes.
            self.stop()

    def _run_vehicle_simulator(self, duration: int = None, max_events: int = None):
        """Run the vehicle simulator in a thread."""
        simulator = VehicleSimulator(
            num_vehicles=self.num_vehicles,
            kafka_bootstrap_servers=self.kafka_bootstrap_servers,
        )
        self.simulators.append(simulator)
        simulator.run(duration_seconds=duration, max_events=max_events)

    def _run_shipment_simulator(self, duration: int = None, max_events: int = None):
        """Run the shipment simulator in a thread."""
        simulator = ShipmentSimulator(
            shipments_per_minute=self.shipments_per_minute,
            kafka_bootstrap_servers=self.kafka_bootstrap_servers,
        )
        self.simulators.append(simulator)
        simulator.run(duration_seconds=duration, max_events=max_events)

    def _run_delivery_simulator(self, duration: int = None, max_events: int = None):
        """Run the delivery simulator in a thread."""
        simulator = DeliverySimulator(
            num_agents=self.num_agents,
            kafka_bootstrap_servers=self.kafka_bootstrap_servers,
        )
        self.simulators.append(simulator)
        simulator.run(duration_seconds=duration, max_events=max_events)

    def start(self, duration: int = None, max_events_per_sim: int = None):
        """Start all simulators."""
        if duration is not None:
            require_positive_int(duration, "duration")
        if max_events_per_sim is not None:
            require_positive_int(max_events_per_sim, "max_events_per_sim")

        logger.info("Starting all simulators...")
        self.running = True

        # Create threads for each simulator
        self.threads = [
            threading.Thread(
                target=self._run_with_guard,
                kwargs={
                    "runner": self._run_vehicle_simulator,
                    "name": "VehicleSimulator",
                    "duration": duration,
                    "max_events": max_events_per_sim,
                },
                name="VehicleSimulator",
                daemon=True,
            ),
            threading.Thread(
                target=self._run_with_guard,
                kwargs={
                    "runner": self._run_shipment_simulator,
                    "name": "ShipmentSimulator",
                    "duration": duration,
                    "max_events": max_events_per_sim,
                },
                name="ShipmentSimulator",
                daemon=True,
            ),
            threading.Thread(
                target=self._run_with_guard,
                kwargs={
                    "runner": self._run_delivery_simulator,
                    "name": "DeliverySimulator",
                    "duration": duration,
                    "max_events": max_events_per_sim,
                },
                name="DeliverySimulator",
                daemon=True,
            ),
        ]

        # Start all threads
        for thread in self.threads:
            thread.start()
            logger.info(f"Started {thread.name}")

    def wait(self):
        """Wait for all simulators to complete."""
        for thread in self.threads:
            thread.join()
        if self.errors:
            raise RuntimeError(f"Simulator failures: {', '.join(sorted(set(self.errors)))}")
        logger.info("All simulators completed")

    def stop(self):
        """Stop all simulators gracefully."""
        logger.info("Stopping all simulators...")
        self.running = False
        for sim in self.simulators:
            sim.close()


def main():
    parser = argparse.ArgumentParser(description="Run all logistics simulators")
    parser.add_argument("--kafka", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--vehicles", type=int, default=50, help="Number of vehicles")
    parser.add_argument("--agents", type=int, default=100, help="Number of delivery agents")
    parser.add_argument("--shipments-rate", type=float, default=10, help="Shipments per minute")
    parser.add_argument("--duration", type=int, help="Duration in seconds")

    args = parser.parse_args()

    orchestrator = SimulatorOrchestrator(
        kafka_bootstrap_servers=args.kafka,
        num_vehicles=args.vehicles,
        num_agents=args.agents,
        shipments_per_minute=args.shipments_rate,
    )

    # Handle graceful shutdown
    def signal_handler(sig, frame):
        logger.info("Received shutdown signal")
        orchestrator.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    orchestrator.start(duration=args.duration)
    try:
        orchestrator.wait()
    except RuntimeError as exc:
        logger.error(str(exc))
        sys.exit(1)


if __name__ == "__main__":
    main()
