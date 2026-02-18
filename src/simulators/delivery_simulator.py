"""
Delivery Agent Simulator - Generates last-mile delivery data.

Simulates:
- Delivery agent GPS positions
- Delivery attempts and completions
- Stop detection patterns
- COD collections
- Customer interactions
"""

import math
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from src.domain.constants import (
    DELIVERY_FAILURE_REASONS,
    TOPIC_AGENT_POSITIONS,
    TOPIC_DELIVERY_EVENTS,
)
from src.utils.geo import bearing_degrees, haversine_distance_km, move_point
from src.utils.validation import require_positive_int, require_positive_number

from .base import BaseSimulator, logger

# Delivery zones in major cities
DELIVERY_ZONES = {
    "DEL": [
        {
            "zone_id": "DEL_Z1",
            "name": "Central Delhi",
            "lat": 28.6139,
            "lng": 77.2090,
            "radius_km": 5,
        },
        {
            "zone_id": "DEL_Z2",
            "name": "South Delhi",
            "lat": 28.5245,
            "lng": 77.2066,
            "radius_km": 6,
        },
        {"zone_id": "DEL_Z3", "name": "East Delhi", "lat": 28.6280, "lng": 77.2950, "radius_km": 5},
        {"zone_id": "DEL_Z4", "name": "Gurgaon", "lat": 28.4595, "lng": 77.0266, "radius_km": 8},
        {"zone_id": "DEL_Z5", "name": "Noida", "lat": 28.5355, "lng": 77.3910, "radius_km": 7},
    ],
    "MUM": [
        {
            "zone_id": "MUM_Z1",
            "name": "Mumbai Central",
            "lat": 18.9712,
            "lng": 72.8197,
            "radius_km": 4,
        },
        {"zone_id": "MUM_Z2", "name": "Andheri", "lat": 19.1136, "lng": 72.8697, "radius_km": 5},
        {"zone_id": "MUM_Z3", "name": "Thane", "lat": 19.2183, "lng": 72.9781, "radius_km": 6},
        {
            "zone_id": "MUM_Z4",
            "name": "Navi Mumbai",
            "lat": 19.0330,
            "lng": 73.0297,
            "radius_km": 7,
        },
    ],
    "BLR": [
        {
            "zone_id": "BLR_Z1",
            "name": "Koramangala",
            "lat": 12.9352,
            "lng": 77.6245,
            "radius_km": 4,
        },
        {"zone_id": "BLR_Z2", "name": "Whitefield", "lat": 12.9698, "lng": 77.7500, "radius_km": 5},
        {
            "zone_id": "BLR_Z3",
            "name": "Electronic City",
            "lat": 12.8399,
            "lng": 77.6770,
            "radius_km": 5,
        },
        {"zone_id": "BLR_Z4", "name": "HSR Layout", "lat": 12.9116, "lng": 77.6389, "radius_km": 4},
    ],
}


@dataclass
class DeliveryOrder:
    """Represents a delivery order assigned to an agent."""

    order_id: str
    shipment_id: str
    customer_id: str
    customer_name: str
    customer_phone: str
    delivery_address: str
    delivery_lat: float
    delivery_lng: float
    is_cod: bool
    cod_amount: float
    status: str = "ASSIGNED"  # ASSIGNED, IN_PROGRESS, DELIVERED, FAILED
    attempts: int = 0
    assigned_time: datetime = None
    completed_time: datetime = None


@dataclass
class DeliveryAgent:
    """Represents a delivery agent."""

    agent_id: str
    name: str
    phone: str
    vehicle_type: str  # BIKE, SCOOTER, BICYCLE
    zone_id: str
    zone_center_lat: float
    zone_center_lng: float
    current_lat: float
    current_lng: float
    current_speed: float = 0.0
    heading: float = 0.0
    status: str = "AVAILABLE"  # AVAILABLE, ON_DELIVERY, BREAK, OFFLINE
    current_order: Optional[DeliveryOrder] = None
    pending_orders: List[DeliveryOrder] = field(default_factory=list)
    completed_deliveries: int = 0
    failed_deliveries: int = 0
    total_cod_collected: float = 0.0
    shift_start: datetime = None
    last_event_time: datetime = None
    is_at_stop: bool = False
    stop_start_time: datetime = None


class DeliverySimulator(BaseSimulator):
    """
    Simulates delivery agents performing last-mile deliveries.

    Features:
    - Agent GPS tracking
    - Order assignment and completion
    - Stop detection
    - Delivery success/failure patterns
    - COD collection tracking
    """

    def __init__(
        self,
        num_agents: int = 100,
        kafka_bootstrap_servers: str = "localhost:9092",
        gps_interval_seconds: float = 30.0,
        **kwargs,
    ):
        require_positive_int(num_agents, "num_agents")
        require_positive_number(gps_interval_seconds, "gps_interval_seconds")

        super().__init__(
            kafka_bootstrap_servers=kafka_bootstrap_servers, topic=TOPIC_AGENT_POSITIONS, **kwargs
        )
        self.num_agents = num_agents
        self.gps_interval_seconds = gps_interval_seconds
        self.agents: List[DeliveryAgent] = []
        self.delivery_topic = TOPIC_DELIVERY_EVENTS
        self._initialize_agents()

    def _initialize_agents(self):
        """Create delivery agents distributed across zones."""
        all_zones = []
        for city_zones in DELIVERY_ZONES.values():
            all_zones.extend(city_zones)

        vehicle_types = ["BIKE"] * 60 + ["SCOOTER"] * 35 + ["BICYCLE"] * 5
        random.shuffle(vehicle_types)

        first_names = [
            "Raj",
            "Amit",
            "Suresh",
            "Rahul",
            "Vijay",
            "Anil",
            "Sanjay",
            "Rakesh",
            "Deepak",
            "Ashok",
            "Manoj",
            "Ramesh",
            "Vikram",
            "Ajay",
            "Ravi",
        ]
        last_names = [
            "Kumar",
            "Singh",
            "Sharma",
            "Verma",
            "Gupta",
            "Patel",
            "Shah",
            "Mehta",
            "Joshi",
            "Rao",
            "Reddy",
            "Nair",
            "Menon",
            "Pillai",
            "Iyer",
        ]

        for i in range(self.num_agents):
            zone = all_zones[i % len(all_zones)]

            # Random position within zone
            angle = random.uniform(0, 2 * math.pi)
            distance = random.uniform(0, zone["radius_km"] * 0.8)
            lat_offset = (distance / 111) * math.cos(angle)
            lng_offset = (distance / (111 * math.cos(math.radians(zone["lat"])))) * math.sin(angle)

            agent = DeliveryAgent(
                agent_id=f"AGT-{str(i+1).zfill(4)}",
                name=f"{random.choice(first_names)} {random.choice(last_names)}",
                phone=f"+91{random.randint(7000000000, 9999999999)}",
                vehicle_type=vehicle_types[i % len(vehicle_types)],
                zone_id=zone["zone_id"],
                zone_center_lat=zone["lat"],
                zone_center_lng=zone["lng"],
                current_lat=zone["lat"] + lat_offset,
                current_lng=zone["lng"] + lng_offset,
                heading=random.uniform(0, 360),
                shift_start=datetime.utcnow(),
                last_event_time=datetime.utcnow(),
            )

            # Assign some initial orders
            num_orders = random.randint(3, 12)
            for _ in range(num_orders):
                order = self._generate_order(agent)
                agent.pending_orders.append(order)

            self.agents.append(agent)

        logger.info(f"Initialized {len(self.agents)} delivery agents across {len(all_zones)} zones")

    def _generate_order(self, agent: DeliveryAgent) -> DeliveryOrder:
        """Generate a delivery order for an agent."""
        # Random delivery location within zone
        angle = random.uniform(0, 2 * math.pi)
        distance = random.uniform(0.5, 3)  # 0.5-3 km from zone center
        lat_offset = (distance / 111) * math.cos(angle)
        lng_offset = (distance / (111 * math.cos(math.radians(agent.zone_center_lat)))) * math.sin(
            angle
        )

        is_cod = random.random() < 0.4

        return DeliveryOrder(
            order_id=f"ORD-{uuid.uuid4().hex[:12].upper()}",
            shipment_id=f"SHP-{uuid.uuid4().hex[:12].upper()}",
            customer_id=f"CST-{random.randint(1, 100000):06d}",
            customer_name=f"Customer {random.randint(1, 10000)}",
            customer_phone=f"+91{random.randint(7000000000, 9999999999)}",
            delivery_address=f"{random.randint(1, 500)}, Block {random.choice('ABCDEFGH')}, Sector {random.randint(1, 50)}",
            delivery_lat=agent.zone_center_lat + lat_offset,
            delivery_lng=agent.zone_center_lng + lng_offset,
            is_cod=is_cod,
            cod_amount=round(random.uniform(200, 3000), 2) if is_cod else 0,
            assigned_time=datetime.utcnow(),
        )

    def _haversine_distance(self, lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """Calculate distance between two points in kilometers."""
        return haversine_distance_km(lat1, lng1, lat2, lng2)

    def _calculate_bearing(self, lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """Calculate bearing from point 1 to point 2."""
        return bearing_degrees(lat1, lng1, lat2, lng2)

    def _move_point(self, lat: float, lng: float, bearing: float, distance_km: float):
        """Move a point by distance in the direction of bearing."""
        return move_point(lat, lng, bearing, distance_km)

    def _get_max_speed(self, agent: DeliveryAgent) -> float:
        """Get max speed based on vehicle type."""
        speeds = {
            "BIKE": 40,
            "SCOOTER": 35,
            "BICYCLE": 20,
        }
        return speeds.get(agent.vehicle_type, 30)

    def _update_agent_position(self, agent: DeliveryAgent):
        """Update agent position based on current state."""
        if agent.status == "OFFLINE" or agent.is_at_stop:
            agent.current_speed = 0
            return

        if agent.current_order:
            # Move towards delivery location
            dest_lat = agent.current_order.delivery_lat
            dest_lng = agent.current_order.delivery_lng

            distance = self._haversine_distance(
                agent.current_lat, agent.current_lng, dest_lat, dest_lng
            )

            if distance < 0.05:  # Within 50m - arrived
                agent.is_at_stop = True
                agent.stop_start_time = datetime.utcnow()
                agent.current_speed = 0
                return

            # Calculate movement
            bearing = self._calculate_bearing(
                agent.current_lat, agent.current_lng, dest_lat, dest_lng
            )
            bearing += random.uniform(-10, 10)  # Road variation
            agent.heading = bearing % 360

            # Speed calculation
            max_speed = self._get_max_speed(agent)
            target_speed = max_speed * random.uniform(0.5, 0.9)

            # Slow down when approaching
            if distance < 0.5:
                target_speed *= distance / 0.5

            agent.current_speed = max(0, target_speed + random.uniform(-5, 5))

            # Move
            distance_km = (agent.current_speed / 3600) * self.gps_interval_seconds
            new_lat, new_lng = self._move_point(
                agent.current_lat, agent.current_lng, agent.heading, distance_km
            )
            agent.current_lat = new_lat
            agent.current_lng = new_lng

        elif agent.pending_orders:
            # Pick up next order
            agent.current_order = agent.pending_orders.pop(0)
            agent.current_order.status = "IN_PROGRESS"
            agent.status = "ON_DELIVERY"

        else:
            # No orders - wander in zone
            agent.heading += random.uniform(-30, 30)
            agent.heading = agent.heading % 360
            agent.current_speed = random.uniform(5, 15)

            distance_km = (agent.current_speed / 3600) * self.gps_interval_seconds
            new_lat, new_lng = self._move_point(
                agent.current_lat, agent.current_lng, agent.heading, distance_km
            )

            # Keep within zone bounds
            zone_dist = self._haversine_distance(
                new_lat, new_lng, agent.zone_center_lat, agent.zone_center_lng
            )
            if zone_dist < 5:  # Stay within 5km of zone center
                agent.current_lat = new_lat
                agent.current_lng = new_lng

    def _process_stop(self, agent: DeliveryAgent) -> Optional[Dict]:
        """Process agent stop (delivery attempt)."""
        if not agent.is_at_stop or not agent.current_order:
            return None

        stop_duration = (datetime.utcnow() - agent.stop_start_time).total_seconds()

        # Average stop duration: 2-8 minutes
        min_stop_duration = random.uniform(60, 120)  # Reduced for simulation

        if stop_duration < min_stop_duration:
            return None

        # Complete the delivery
        order = agent.current_order
        order.attempts += 1

        # 85% success rate
        success = random.random() < 0.85

        if success:
            order.status = "DELIVERED"
            order.completed_time = datetime.utcnow()
            agent.completed_deliveries += 1

            if order.is_cod:
                agent.total_cod_collected += order.cod_amount

            event = self._generate_delivery_event(agent, order, "DELIVERED")

        else:
            failure_reason = random.choice(
                [
                    reason
                    for reason in DELIVERY_FAILURE_REASONS
                    if reason not in {"DAMAGED_PACKAGE", "OTHER"}
                ]
            )
            if not order.is_cod and failure_reason == "PAYMENT_ISSUE":
                failure_reason = "CUSTOMER_NOT_AVAILABLE"

            if order.attempts >= 3:
                order.status = "FAILED"
                agent.failed_deliveries += 1
                event = self._generate_delivery_event(
                    agent, order, "DELIVERY_FAILED", failure_reason
                )
            else:
                # Will retry later - put back in queue
                order.status = "ASSIGNED"
                agent.pending_orders.append(order)
                event = self._generate_delivery_event(
                    agent, order, "DELIVERY_ATTEMPTED", failure_reason
                )

        # Reset stop state
        agent.is_at_stop = False
        agent.stop_start_time = None
        agent.current_order = None
        agent.status = "AVAILABLE"

        # Add new order occasionally
        if random.random() < 0.3:
            new_order = self._generate_order(agent)
            agent.pending_orders.append(new_order)

        return event

    def _generate_delivery_event(
        self,
        agent: DeliveryAgent,
        order: DeliveryOrder,
        event_type: str,
        failure_reason: str = None,
    ) -> Dict[str, Any]:
        """Generate a delivery event."""
        pod_type = None
        customer_rating = None

        if event_type == "DELIVERED":
            pod_type = random.choice(["OTP", "SIGNATURE", "PHOTO"])
            if random.random() < 0.3:  # 30% leave rating
                customer_rating = random.choices([5, 4, 3, 2, 1], weights=[50, 30, 10, 7, 3])[0]

        return {
            "event_id": f"DLV-{uuid.uuid4().hex[:12].upper()}",
            "event_type": event_type,
            "timestamp": self._get_timestamp(),
            "agent_id": agent.agent_id,
            "agent_name": agent.name,
            "order_id": order.order_id,
            "shipment_id": order.shipment_id,
            "customer_id": order.customer_id,
            "delivery_lat": order.delivery_lat,
            "delivery_lng": order.delivery_lng,
            "delivery_address": order.delivery_address,
            "zone_id": agent.zone_id,
            "is_cod": order.is_cod,
            "cod_amount": order.cod_amount if order.is_cod else None,
            "cod_collected": (
                order.cod_amount if (event_type == "DELIVERED" and order.is_cod) else None
            ),
            "payment_mode": (
                random.choice(["CASH", "UPI"])
                if (event_type == "DELIVERED" and order.is_cod)
                else None
            ),
            "attempt_number": order.attempts,
            "failure_reason": failure_reason,
            "pod_type": pod_type,
            "customer_rating": customer_rating,
            "time_at_location_seconds": (
                int((datetime.utcnow() - agent.stop_start_time).total_seconds())
                if agent.stop_start_time
                else None
            ),
        }

    def generate_event(self, agent: DeliveryAgent = None) -> Dict[str, Any]:
        """Generate a GPS position event for an agent."""
        if agent is None:
            agent = random.choice(self.agents)

        # Add GPS noise
        lat_noise = random.gauss(0, 0.00003)
        lng_noise = random.gauss(0, 0.00003)

        return {
            "event_id": f"POS-{uuid.uuid4().hex[:12].upper()}",
            "agent_id": agent.agent_id,
            "timestamp": self._get_timestamp(),
            "latitude": round(agent.current_lat + lat_noise, 6),
            "longitude": round(agent.current_lng + lng_noise, 6),
            "speed_kmh": round(agent.current_speed, 1),
            "heading": round(agent.heading, 1),
            "accuracy_m": round(random.uniform(3, 10), 1),
            "zone_id": agent.zone_id,
            "vehicle_type": agent.vehicle_type,
            "status": agent.status,
            "is_at_stop": agent.is_at_stop,
            "current_order_id": agent.current_order.order_id if agent.current_order else None,
            "pending_orders": len(agent.pending_orders),
            "completed_today": agent.completed_deliveries,
            "failed_today": agent.failed_deliveries,
            "battery_pct": random.randint(20, 100),  # Phone battery
        }

    def run(self, duration_seconds: int = None, max_events: int = None):
        """Run the delivery agent simulator."""
        if duration_seconds is not None:
            require_positive_int(duration_seconds, "duration_seconds")
        if max_events is not None:
            require_positive_int(max_events, "max_events")

        if not self.connect():
            logger.error("Failed to connect to Kafka, running in dry-run mode")

        self.start_time = time.time()
        logger.info(f"Starting delivery simulator with {self.num_agents} agents")

        try:
            while True:
                cycle_start = time.time()

                for agent in self.agents:
                    try:
                        # Update position
                        self._update_agent_position(agent)

                        # Generate GPS event
                        gps_event = self.generate_event(agent)
                        if self.producer:
                            self.send(gps_event, key=agent.agent_id)

                        # Process any stops (deliveries)
                        delivery_event = self._process_stop(agent)
                        if delivery_event and self.producer:
                            self.producer.send(
                                self.delivery_topic, key=agent.agent_id, value=delivery_event
                            )
                    except Exception:
                        logger.exception("Failed to process agent %s", agent.agent_id)

                # Log stats
                if self.message_count % 200 == 0 and self.message_count > 0:
                    self._log_stats()
                    on_delivery = sum(1 for a in self.agents if a.status == "ON_DELIVERY")
                    total_completed = sum(a.completed_deliveries for a in self.agents)
                    logger.info(
                        f"On delivery: {on_delivery}/{self.num_agents}, Total completed: {total_completed}"
                    )

                # Check termination
                if duration_seconds and (time.time() - self.start_time) >= duration_seconds:
                    logger.info(f"Duration limit reached ({duration_seconds}s)")
                    break

                if max_events and self.message_count >= max_events:
                    logger.info(f"Event limit reached ({max_events})")
                    break

                # Sleep to maintain interval
                elapsed = time.time() - cycle_start
                sleep_time = max(0, self.gps_interval_seconds - elapsed)
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Simulator stopped by user")
        finally:
            self.close()


def main():
    """Run the delivery simulator standalone."""
    import argparse

    parser = argparse.ArgumentParser(description="Delivery Agent Simulator")
    parser.add_argument("--agents", type=int, default=100, help="Number of agents")
    parser.add_argument("--kafka", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--duration", type=int, help="Duration in seconds")
    parser.add_argument("--interval", type=float, default=30.0, help="GPS interval in seconds")

    args = parser.parse_args()

    simulator = DeliverySimulator(
        num_agents=args.agents,
        kafka_bootstrap_servers=args.kafka,
        gps_interval_seconds=args.interval,
    )
    simulator.run(duration_seconds=args.duration)


if __name__ == "__main__":
    main()
