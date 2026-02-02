"""
Shipment Event Simulator - Generates package tracking events.

Simulates:
- Shipment lifecycle from creation to delivery
- Hub network routing (scan events at each hub)
- SLA tracking and delays
- Failed deliveries and returns
"""

import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

from .base import BaseSimulator, logger


class ShipmentState(Enum):
    """Shipment lifecycle states."""
    CREATED = "CREATED"
    PICKUP_SCHEDULED = "PICKUP_SCHEDULED"
    PICKED_UP = "PICKED_UP"
    IN_TRANSIT = "IN_TRANSIT"
    HUB_ARRIVED = "HUB_ARRIVED"
    HUB_INSCAN = "HUB_INSCAN"
    HUB_SORTED = "HUB_SORTED"
    HUB_OUTSCAN = "HUB_OUTSCAN"
    HUB_DEPARTED = "HUB_DEPARTED"
    OUT_FOR_DELIVERY = "OUT_FOR_DELIVERY"
    DELIVERY_ATTEMPTED = "DELIVERY_ATTEMPTED"
    DELIVERED = "DELIVERED"
    DELIVERY_FAILED = "DELIVERY_FAILED"
    RETURNED_TO_ORIGIN = "RETURNED_TO_ORIGIN"


# Hub network with connections
HUB_NETWORK = {
    "HUB_DEL_01": {
        "name": "Delhi Hub",
        "city": "Delhi",
        "lat": 28.5505,
        "lng": 77.2506,
        "type": "MEGA",
        "connections": ["HUB_JAI_01", "HUB_LKO_01", "HUB_MUM_01", "HUB_KOL_01"],
        "processing_hours": (1, 4),
    },
    "HUB_MUM_01": {
        "name": "Mumbai Hub",
        "city": "Mumbai",
        "lat": 19.0330,
        "lng": 72.8520,
        "type": "MEGA",
        "connections": ["HUB_DEL_01", "HUB_PUN_01", "HUB_BLR_01", "HUB_AMD_01"],
        "processing_hours": (1, 4),
    },
    "HUB_BLR_01": {
        "name": "Bangalore Hub",
        "city": "Bangalore",
        "lat": 13.0100,
        "lng": 77.5500,
        "type": "MEGA",
        "connections": ["HUB_CHN_01", "HUB_HYD_01", "HUB_MUM_01"],
        "processing_hours": (1, 3),
    },
    "HUB_CHN_01": {
        "name": "Chennai Hub",
        "city": "Chennai",
        "lat": 13.0600,
        "lng": 80.2100,
        "type": "MEGA",
        "connections": ["HUB_BLR_01", "HUB_HYD_01"],
        "processing_hours": (1, 4),
    },
    "HUB_HYD_01": {
        "name": "Hyderabad Hub",
        "city": "Hyderabad",
        "lat": 17.4400,
        "lng": 78.3800,
        "type": "MEGA",
        "connections": ["HUB_BLR_01", "HUB_CHN_01", "HUB_MUM_01", "HUB_DEL_01"],
        "processing_hours": (1, 3),
    },
    "HUB_KOL_01": {
        "name": "Kolkata Hub",
        "city": "Kolkata",
        "lat": 22.5726,
        "lng": 88.3639,
        "type": "MEGA",
        "connections": ["HUB_DEL_01"],
        "processing_hours": (2, 5),
    },
    "HUB_PUN_01": {
        "name": "Pune Hub",
        "city": "Pune",
        "lat": 18.5204,
        "lng": 73.8567,
        "type": "REGIONAL",
        "connections": ["HUB_MUM_01", "HUB_BLR_01"],
        "processing_hours": (2, 5),
    },
    "HUB_AMD_01": {
        "name": "Ahmedabad Hub",
        "city": "Ahmedabad",
        "lat": 23.0225,
        "lng": 72.5714,
        "type": "REGIONAL",
        "connections": ["HUB_MUM_01", "HUB_DEL_01"],
        "processing_hours": (2, 5),
    },
    "HUB_JAI_01": {
        "name": "Jaipur Hub",
        "city": "Jaipur",
        "lat": 26.9124,
        "lng": 75.7873,
        "type": "REGIONAL",
        "connections": ["HUB_DEL_01", "HUB_AMD_01"],
        "processing_hours": (2, 6),
    },
    "HUB_LKO_01": {
        "name": "Lucknow Hub",
        "city": "Lucknow",
        "lat": 26.8467,
        "lng": 80.9462,
        "type": "REGIONAL",
        "connections": ["HUB_DEL_01", "HUB_KOL_01"],
        "processing_hours": (2, 6),
    },
}


@dataclass
class Shipment:
    """Represents a single shipment."""
    shipment_id: str
    awb_number: str  # Air Waybill number
    seller_id: str
    customer_id: str
    origin_hub: str
    destination_hub: str
    current_hub: Optional[str] = None
    route: List[str] = field(default_factory=list)
    current_route_index: int = 0
    state: ShipmentState = ShipmentState.CREATED
    created_at: datetime = field(default_factory=datetime.utcnow)
    promised_delivery: datetime = None
    weight_kg: float = 1.0
    dimensions_cm: Dict = field(default_factory=lambda: {"l": 20, "w": 15, "h": 10})
    cod_amount: float = 0.0
    is_cod: bool = False
    delivery_attempts: int = 0
    last_event_time: datetime = None
    next_event_time: datetime = None


class ShipmentSimulator(BaseSimulator):
    """
    Simulates shipment tracking events through a hub network.

    Features:
    - Realistic state machine transitions
    - Hub-to-hub routing
    - Scan events at each processing stage
    - SLA tracking
    - Failed delivery simulation
    """

    def __init__(
        self,
        shipments_per_minute: float = 10,
        kafka_bootstrap_servers: str = "localhost:9092",
        **kwargs
    ):
        super().__init__(
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            topic="shipment_events",
            **kwargs
        )
        self.shipments_per_minute = shipments_per_minute
        self.active_shipments: Dict[str, Shipment] = {}
        self.completed_shipments: List[str] = []
        self.hub_network = HUB_NETWORK

    def _generate_awb(self) -> str:
        """Generate a realistic AWB number."""
        prefix = random.choice(["DEL", "MUM", "BLR", "CHN", "HYD"])
        number = random.randint(100000000, 999999999)
        return f"{prefix}{number}"

    def _find_route(self, origin: str, destination: str) -> List[str]:
        """Find a route between two hubs using BFS."""
        if origin == destination:
            return [origin]

        visited = {origin}
        queue = [[origin]]

        while queue:
            path = queue.pop(0)
            current = path[-1]

            for neighbor in self.hub_network[current]["connections"]:
                if neighbor == destination:
                    return path + [neighbor]

                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(path + [neighbor])

        # Fallback: direct route (shouldn't happen with connected network)
        return [origin, destination]

    def _create_shipment(self) -> Shipment:
        """Create a new shipment with random origin/destination."""
        hubs = list(self.hub_network.keys())
        origin = random.choice(hubs)
        destination = random.choice([h for h in hubs if h != origin])

        route = self._find_route(origin, destination)

        # SLA based on route length
        sla_days = max(2, len(route))  # Min 2 days
        if random.random() < 0.2:  # 20% express
            sla_days = max(1, sla_days - 1)

        shipment = Shipment(
            shipment_id=f"SHP-{uuid.uuid4().hex[:12].upper()}",
            awb_number=self._generate_awb(),
            seller_id=f"SLR-{random.randint(1, 1000):05d}",
            customer_id=f"CST-{random.randint(1, 50000):06d}",
            origin_hub=origin,
            destination_hub=destination,
            current_hub=origin,
            route=route,
            promised_delivery=datetime.utcnow() + timedelta(days=sla_days),
            weight_kg=round(random.uniform(0.1, 30), 2),
            cod_amount=round(random.uniform(100, 5000), 2) if random.random() < 0.4 else 0,
            is_cod=random.random() < 0.4,
            last_event_time=datetime.utcnow(),
            next_event_time=datetime.utcnow() + timedelta(minutes=random.randint(5, 30)),
        )
        shipment.dimensions_cm = {
            "l": random.randint(10, 60),
            "w": random.randint(10, 40),
            "h": random.randint(5, 30),
        }

        return shipment

    def _get_next_state(self, shipment: Shipment) -> Optional[ShipmentState]:
        """Determine the next state in the shipment lifecycle."""
        state = shipment.state

        # State machine transitions
        transitions = {
            ShipmentState.CREATED: ShipmentState.PICKUP_SCHEDULED,
            ShipmentState.PICKUP_SCHEDULED: ShipmentState.PICKED_UP,
            ShipmentState.PICKED_UP: ShipmentState.HUB_ARRIVED,
            ShipmentState.HUB_ARRIVED: ShipmentState.HUB_INSCAN,
            ShipmentState.HUB_INSCAN: ShipmentState.HUB_SORTED,
            ShipmentState.HUB_SORTED: ShipmentState.HUB_OUTSCAN,
            ShipmentState.HUB_OUTSCAN: ShipmentState.HUB_DEPARTED,
            ShipmentState.HUB_DEPARTED: ShipmentState.IN_TRANSIT,
        }

        if state in transitions:
            return transitions[state]

        if state == ShipmentState.IN_TRANSIT:
            # Check if we've reached the destination hub
            if shipment.current_route_index >= len(shipment.route) - 1:
                return ShipmentState.OUT_FOR_DELIVERY
            else:
                return ShipmentState.HUB_ARRIVED

        if state == ShipmentState.OUT_FOR_DELIVERY:
            # 85% success rate, 15% failure
            if random.random() < 0.85:
                return ShipmentState.DELIVERED
            else:
                return ShipmentState.DELIVERY_ATTEMPTED

        if state == ShipmentState.DELIVERY_ATTEMPTED:
            shipment.delivery_attempts += 1
            if shipment.delivery_attempts >= 3:
                return ShipmentState.DELIVERY_FAILED
            else:
                return ShipmentState.OUT_FOR_DELIVERY

        if state == ShipmentState.DELIVERY_FAILED:
            return ShipmentState.RETURNED_TO_ORIGIN

        return None

    def _calculate_next_event_time(self, shipment: Shipment) -> datetime:
        """Calculate when the next event should occur."""
        state = shipment.state

        # Time delays in minutes for each state
        delays = {
            ShipmentState.CREATED: (5, 30),
            ShipmentState.PICKUP_SCHEDULED: (60, 240),  # 1-4 hours
            ShipmentState.PICKED_UP: (30, 120),
            ShipmentState.HUB_ARRIVED: (5, 15),
            ShipmentState.HUB_INSCAN: (10, 30),
            ShipmentState.HUB_SORTED: (30, 120),
            ShipmentState.HUB_OUTSCAN: (10, 30),
            ShipmentState.HUB_DEPARTED: (5, 15),
            ShipmentState.IN_TRANSIT: (120, 720),  # 2-12 hours between hubs
            ShipmentState.OUT_FOR_DELIVERY: (60, 480),  # 1-8 hours
            ShipmentState.DELIVERY_ATTEMPTED: (240, 1440),  # 4-24 hours retry
        }

        min_delay, max_delay = delays.get(state, (30, 120))

        # Speed up for simulation (divide by 60 for demo)
        min_delay = max(1, min_delay // 60)
        max_delay = max(2, max_delay // 60)

        return datetime.utcnow() + timedelta(minutes=random.randint(min_delay, max_delay))

    def generate_event(self, shipment: Shipment = None) -> Dict[str, Any]:
        """Generate a shipment event."""
        if shipment is None:
            shipment = self._create_shipment()
            self.active_shipments[shipment.shipment_id] = shipment

        hub = self.hub_network.get(shipment.current_hub, {})

        # Determine failure reason if applicable
        failure_reason = None
        if shipment.state == ShipmentState.DELIVERY_ATTEMPTED:
            failure_reason = random.choice([
                "CUSTOMER_NOT_AVAILABLE",
                "WRONG_ADDRESS",
                "ACCESS_RESTRICTED",
                "CUSTOMER_REFUSED",
                "PAYMENT_ISSUE",
            ])

        event = {
            "event_id": f"EVT-{uuid.uuid4().hex[:12].upper()}",
            "shipment_id": shipment.shipment_id,
            "awb_number": shipment.awb_number,
            "timestamp": self._get_timestamp(),
            "event_type": shipment.state.value,
            "hub_id": shipment.current_hub,
            "hub_name": hub.get("name", "Unknown"),
            "hub_city": hub.get("city", "Unknown"),
            "latitude": hub.get("lat", 0) + random.uniform(-0.01, 0.01),
            "longitude": hub.get("lng", 0) + random.uniform(-0.01, 0.01),
            "seller_id": shipment.seller_id,
            "customer_id": shipment.customer_id,
            "origin_hub": shipment.origin_hub,
            "destination_hub": shipment.destination_hub,
            "weight_kg": shipment.weight_kg,
            "is_cod": shipment.is_cod,
            "cod_amount": shipment.cod_amount if shipment.is_cod else None,
            "promised_delivery": shipment.promised_delivery.isoformat() + "Z",
            "route_hops": len(shipment.route),
            "current_hop": shipment.current_route_index + 1,
            "delivery_attempts": shipment.delivery_attempts,
            "failure_reason": failure_reason,
            "scanner_id": f"SCN-{random.randint(1, 100):03d}",
            "worker_id": f"WRK-{random.randint(1, 500):04d}",
        }

        return event

    def _process_shipment(self, shipment: Shipment) -> Optional[Dict]:
        """Process a shipment state transition and generate event."""
        now = datetime.utcnow()

        # Check if it's time for next event
        if shipment.next_event_time and now < shipment.next_event_time:
            return None

        # Get next state
        next_state = self._get_next_state(shipment)
        if next_state is None:
            return None

        # Update state
        shipment.state = next_state
        shipment.last_event_time = now

        # Handle hub transitions
        if next_state == ShipmentState.IN_TRANSIT:
            # Move to next hub in route
            if shipment.current_route_index < len(shipment.route) - 1:
                shipment.current_route_index += 1
                shipment.current_hub = shipment.route[shipment.current_route_index]

        # Calculate next event time
        shipment.next_event_time = self._calculate_next_event_time(shipment)

        # Generate event
        return self.generate_event(shipment)

    def _is_shipment_complete(self, shipment: Shipment) -> bool:
        """Check if shipment has reached a terminal state."""
        terminal_states = {
            ShipmentState.DELIVERED,
            ShipmentState.DELIVERY_FAILED,
            ShipmentState.RETURNED_TO_ORIGIN,
        }
        return shipment.state in terminal_states

    def run(self, duration_seconds: int = None, max_events: int = None):
        """
        Run the shipment simulator.

        Args:
            duration_seconds: How long to run (None = forever)
            max_events: Max events to generate (None = unlimited)
        """
        if not self.connect():
            logger.error("Failed to connect to Kafka, running in dry-run mode")

        self.start_time = time.time()
        last_shipment_time = time.time()
        shipment_interval = 60.0 / self.shipments_per_minute

        logger.info(f"Starting shipment simulator ({self.shipments_per_minute} shipments/min)")

        try:
            while True:
                now = time.time()

                # Create new shipments at configured rate
                if now - last_shipment_time >= shipment_interval:
                    shipment = self._create_shipment()
                    self.active_shipments[shipment.shipment_id] = shipment

                    event = self.generate_event(shipment)
                    if self.producer:
                        self.send(event, key=shipment.shipment_id)

                    last_shipment_time = now
                    logger.debug(f"Created shipment {shipment.shipment_id}: {shipment.origin_hub} -> {shipment.destination_hub}")

                # Process existing shipments
                completed = []
                for shipment_id, shipment in self.active_shipments.items():
                    event = self._process_shipment(shipment)
                    if event:
                        if self.producer:
                            self.send(event, key=shipment_id)

                        if self._is_shipment_complete(shipment):
                            completed.append(shipment_id)
                            logger.debug(f"Shipment {shipment_id} completed: {shipment.state.value}")

                # Remove completed shipments
                for shipment_id in completed:
                    del self.active_shipments[shipment_id]
                    self.completed_shipments.append(shipment_id)

                # Log stats periodically
                if self.message_count % 100 == 0 and self.message_count > 0:
                    self._log_stats()
                    logger.info(f"Active shipments: {len(self.active_shipments)}, Completed: {len(self.completed_shipments)}")

                # Check termination conditions
                if duration_seconds and (now - self.start_time) >= duration_seconds:
                    logger.info(f"Duration limit reached ({duration_seconds}s)")
                    break

                if max_events and self.message_count >= max_events:
                    logger.info(f"Event limit reached ({max_events})")
                    break

                # Small sleep to prevent CPU spinning
                time.sleep(0.1)

        except KeyboardInterrupt:
            logger.info("Simulator stopped by user")
        finally:
            self.close()


def main():
    """Run the shipment simulator standalone."""
    import argparse

    parser = argparse.ArgumentParser(description="Shipment Event Simulator")
    parser.add_argument("--rate", type=float, default=10, help="Shipments per minute")
    parser.add_argument("--kafka", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--duration", type=int, help="Duration in seconds")

    args = parser.parse_args()

    simulator = ShipmentSimulator(
        shipments_per_minute=args.rate,
        kafka_bootstrap_servers=args.kafka,
    )
    simulator.run(duration_seconds=args.duration)


if __name__ == "__main__":
    main()
