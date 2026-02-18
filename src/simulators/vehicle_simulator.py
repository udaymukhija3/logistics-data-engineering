"""
Vehicle GPS Simulator - Generates realistic fleet telematics data.

Simulates:
- GPS positions every 10 seconds
- Vehicle telemetry (speed, fuel, engine metrics)
- Realistic driving patterns (stops, routes, traffic)
- Driving events (speeding, harsh braking, idle)
"""

import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from src.domain.constants import TOPIC_ALERTS, TOPIC_VEHICLE_POSITIONS, TOPIC_VEHICLE_TELEMETRY
from src.utils.geo import bearing_degrees, haversine_distance_km, move_point
from src.utils.validation import require_positive_int, require_positive_number

from .base import BaseSimulator, logger

# Indian city coordinates for realistic routes
INDIAN_CITIES = {
    "delhi": {"lat": 28.6139, "lng": 77.2090},
    "mumbai": {"lat": 19.0760, "lng": 72.8777},
    "bangalore": {"lat": 12.9716, "lng": 77.5946},
    "chennai": {"lat": 13.0827, "lng": 80.2707},
    "hyderabad": {"lat": 17.3850, "lng": 78.4867},
    "kolkata": {"lat": 22.5726, "lng": 88.3639},
    "pune": {"lat": 18.5204, "lng": 73.8567},
    "ahmedabad": {"lat": 23.0225, "lng": 72.5714},
    "jaipur": {"lat": 26.9124, "lng": 75.7873},
    "lucknow": {"lat": 26.8467, "lng": 80.9462},
}

# Hub locations (subset of cities with warehouses)
HUB_LOCATIONS = [
    {"id": "HUB_DEL_01", "name": "Delhi Hub", "lat": 28.5505, "lng": 77.2506},
    {"id": "HUB_MUM_01", "name": "Mumbai Hub", "lat": 19.0330, "lng": 72.8520},
    {"id": "HUB_BLR_01", "name": "Bangalore Hub", "lat": 13.0100, "lng": 77.5500},
    {"id": "HUB_HYD_01", "name": "Hyderabad Hub", "lat": 17.4400, "lng": 78.3800},
    {"id": "HUB_CHN_01", "name": "Chennai Hub", "lat": 13.0600, "lng": 80.2100},
]


@dataclass
class Vehicle:
    """Represents a single vehicle in the fleet."""

    vehicle_id: str
    vehicle_type: str  # TRUCK, VAN, BIKE
    driver_id: str
    current_lat: float
    current_lng: float
    current_speed: float = 0.0
    heading: float = 0.0  # Degrees from north
    fuel_level: float = 100.0
    engine_rpm: int = 0
    engine_temp: float = 85.0
    odometer: float = 0.0
    trip_id: Optional[str] = None
    destination: Optional[Dict] = None
    state: str = "IDLE"  # IDLE, MOVING, STOPPED
    last_stop_time: Optional[datetime] = None
    events_generated: int = 0


class VehicleSimulator(BaseSimulator):
    """
    Simulates a fleet of vehicles generating GPS and telemetry data.

    Features:
    - Realistic movement patterns between hubs
    - Traffic simulation (slower during peak hours)
    - Stop patterns (loading/unloading, breaks)
    - Driving events (speeding, harsh braking)
    - Fuel consumption modeling
    """

    def __init__(
        self,
        num_vehicles: int = 50,
        kafka_bootstrap_servers: str = "localhost:9092",
        gps_interval_seconds: float = 10.0,
        **kwargs,
    ):
        require_positive_int(num_vehicles, "num_vehicles")
        require_positive_number(gps_interval_seconds, "gps_interval_seconds")

        super().__init__(
            kafka_bootstrap_servers=kafka_bootstrap_servers, topic=TOPIC_VEHICLE_POSITIONS, **kwargs
        )
        self.num_vehicles = num_vehicles
        self.gps_interval_seconds = gps_interval_seconds
        self.vehicles: List[Vehicle] = []
        self.telemetry_topic = TOPIC_VEHICLE_TELEMETRY
        self._initialize_fleet()

    def _initialize_fleet(self):
        """Create the vehicle fleet with initial positions."""
        vehicle_types = ["TRUCK"] * 30 + ["VAN"] * 15 + ["BIKE"] * 5
        random.shuffle(vehicle_types)

        for i in range(self.num_vehicles):
            # Start vehicles at random hub locations
            hub = random.choice(HUB_LOCATIONS)

            # Add some randomness to starting position
            lat = hub["lat"] + random.uniform(-0.05, 0.05)
            lng = hub["lng"] + random.uniform(-0.05, 0.05)

            vehicle = Vehicle(
                vehicle_id=f"VH-{str(i+1).zfill(4)}",
                vehicle_type=vehicle_types[i % len(vehicle_types)],
                driver_id=f"DRV-{str(random.randint(1, 200)).zfill(4)}",
                current_lat=lat,
                current_lng=lng,
                heading=random.uniform(0, 360),
                fuel_level=random.uniform(40, 100),
                odometer=random.uniform(10000, 150000),
            )
            self.vehicles.append(vehicle)

        logger.info(f"Initialized fleet with {len(self.vehicles)} vehicles")

    def _get_traffic_factor(self) -> float:
        """Return speed reduction factor based on time of day."""
        hour = datetime.now().hour

        # Peak hours: 8-10 AM and 5-8 PM
        if 8 <= hour <= 10 or 17 <= hour <= 20:
            return random.uniform(0.4, 0.6)  # 40-60% of normal speed
        # Night: faster
        elif 22 <= hour or hour <= 5:
            return random.uniform(0.9, 1.0)
        # Normal hours
        else:
            return random.uniform(0.7, 0.85)

    def _calculate_max_speed(self, vehicle: Vehicle) -> float:
        """Get max speed based on vehicle type."""
        base_speeds = {
            "TRUCK": 60,  # km/h
            "VAN": 70,
            "BIKE": 50,
        }
        return base_speeds.get(vehicle.vehicle_type, 60)

    def _should_stop(self, vehicle: Vehicle) -> bool:
        """Determine if vehicle should make a stop."""
        # Random chance of stopping (simulates traffic lights, loading, etc.)
        if vehicle.state == "MOVING":
            # 2% chance per tick of stopping
            if random.random() < 0.02:
                return True

            # Check if near destination
            if vehicle.destination:
                dist = self._haversine_distance(
                    vehicle.current_lat,
                    vehicle.current_lng,
                    vehicle.destination["lat"],
                    vehicle.destination["lng"],
                )
                if dist < 0.5:  # Within 500m of destination
                    return True
        return False

    def _should_start_moving(self, vehicle: Vehicle) -> bool:
        """Determine if stopped vehicle should start moving."""
        if vehicle.state in ["IDLE", "STOPPED"] and vehicle.last_stop_time:
            stop_duration = (datetime.now() - vehicle.last_stop_time).total_seconds()

            # Average stop is 5-30 minutes
            if stop_duration > random.uniform(300, 1800):
                return True
        elif vehicle.state == "IDLE":
            # Idle vehicles have 5% chance of starting a trip
            return random.random() < 0.05
        return False

    def _assign_destination(self, vehicle: Vehicle):
        """Assign a new destination to the vehicle."""
        # Pick a different hub as destination
        current_hub = min(
            HUB_LOCATIONS,
            key=lambda h: self._haversine_distance(
                vehicle.current_lat, vehicle.current_lng, h["lat"], h["lng"]
            ),
        )

        other_hubs = [h for h in HUB_LOCATIONS if h["id"] != current_hub["id"]]
        destination = random.choice(other_hubs)

        vehicle.destination = destination
        vehicle.trip_id = f"TRIP-{uuid.uuid4().hex[:12].upper()}"
        vehicle.state = "MOVING"

        logger.debug(f"Vehicle {vehicle.vehicle_id} starting trip to {destination['name']}")

    def _update_position(self, vehicle: Vehicle):
        """Update vehicle position based on current state."""
        if vehicle.state != "MOVING" or not vehicle.destination:
            return

        # Calculate bearing to destination
        dest_lat = vehicle.destination["lat"]
        dest_lng = vehicle.destination["lng"]

        bearing = self._calculate_bearing(
            vehicle.current_lat, vehicle.current_lng, dest_lat, dest_lng
        )

        # Add some randomness to simulate road curves
        bearing += random.uniform(-15, 15)
        vehicle.heading = bearing % 360

        # Calculate speed
        traffic_factor = self._get_traffic_factor()
        max_speed = self._calculate_max_speed(vehicle)
        target_speed = max_speed * traffic_factor

        # Gradually adjust speed (acceleration/deceleration)
        speed_diff = target_speed - vehicle.current_speed
        vehicle.current_speed += speed_diff * 0.3 + random.uniform(-5, 5)
        vehicle.current_speed = max(0, min(vehicle.current_speed, max_speed * 1.2))

        # Calculate distance traveled in this interval
        # speed in km/h, interval in seconds -> distance in km
        distance_km = (vehicle.current_speed / 3600) * self.gps_interval_seconds

        # Update position
        new_lat, new_lng = self._move_point(
            vehicle.current_lat, vehicle.current_lng, bearing, distance_km
        )

        vehicle.current_lat = new_lat
        vehicle.current_lng = new_lng
        vehicle.odometer += distance_km

        # Update fuel (rough estimate: 0.1L per km for trucks)
        fuel_consumption = {
            "TRUCK": 0.15,
            "VAN": 0.10,
            "BIKE": 0.03,
        }
        vehicle.fuel_level -= distance_km * fuel_consumption.get(vehicle.vehicle_type, 0.1)
        vehicle.fuel_level = max(5, vehicle.fuel_level)  # Min 5% fuel

    def _haversine_distance(
        self,
        lat1: float,
        lng1: float,
        lat2: float,
        lng2: float,
    ) -> float:
        """Calculate distance between two points in kilometers."""
        return haversine_distance_km(lat1, lng1, lat2, lng2)

    def _calculate_bearing(self, lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """Calculate bearing from point 1 to point 2 in degrees."""
        return bearing_degrees(lat1, lng1, lat2, lng2)

    def _move_point(self, lat: float, lng: float, bearing: float, distance_km: float):
        """Move a point by distance in the direction of bearing."""
        return move_point(lat, lng, bearing, distance_km)

    def _detect_driving_event(self, vehicle: Vehicle, prev_speed: float) -> Optional[Dict]:
        """Detect driving events like speeding, harsh braking."""
        events = []

        # Speeding detection
        speed_limit = 80 if vehicle.vehicle_type == "TRUCK" else 70
        if vehicle.current_speed > speed_limit:
            severity = "WARNING" if vehicle.current_speed < speed_limit + 20 else "HIGH"
            events.append(
                {
                    "event_type": "SPEEDING",
                    "severity": severity,
                    "speed": round(vehicle.current_speed, 1),
                    "speed_limit": speed_limit,
                    "overspeed_by": round(vehicle.current_speed - speed_limit, 1),
                }
            )

        # Harsh braking detection (deceleration > 4 m/s²)
        speed_change = prev_speed - vehicle.current_speed  # km/h
        decel_ms2 = (speed_change * 1000 / 3600) / self.gps_interval_seconds

        if decel_ms2 > 4:
            events.append(
                {
                    "event_type": "HARSH_BRAKING",
                    "severity": "WARNING" if decel_ms2 < 6 else "HIGH",
                    "deceleration_ms2": round(decel_ms2, 2),
                }
            )

        # Harsh acceleration
        if decel_ms2 < -3.5:
            events.append(
                {
                    "event_type": "HARSH_ACCELERATION",
                    "severity": "INFO",
                    "acceleration_ms2": round(-decel_ms2, 2),
                }
            )

        return events[0] if events else None

    def generate_event(self, vehicle: Vehicle = None) -> Dict[str, Any]:
        """Generate a GPS position event for a vehicle."""
        if vehicle is None:
            vehicle = random.choice(self.vehicles)

        # Add GPS noise (realistic GPS accuracy ~5-10m)
        lat_noise = random.gauss(0, 0.00005)  # ~5m
        lng_noise = random.gauss(0, 0.00005)

        event = {
            "event_id": f"GPS-{uuid.uuid4().hex[:12].upper()}",
            "vehicle_id": vehicle.vehicle_id,
            "driver_id": vehicle.driver_id,
            "vehicle_type": vehicle.vehicle_type,
            "timestamp": self._get_timestamp(),
            "latitude": round(vehicle.current_lat + lat_noise, 6),
            "longitude": round(vehicle.current_lng + lng_noise, 6),
            "speed_kmh": round(vehicle.current_speed, 1),
            "heading": round(vehicle.heading, 1),
            "altitude_m": random.randint(200, 500),  # Simplified
            "accuracy_m": round(random.uniform(3, 15), 1),
            "trip_id": vehicle.trip_id,
            "state": vehicle.state,
            "fuel_level_pct": round(vehicle.fuel_level, 1),
            "odometer_km": round(vehicle.odometer, 1),
        }

        vehicle.events_generated += 1
        return event

    def generate_telemetry(self, vehicle: Vehicle) -> Dict[str, Any]:
        """Generate telemetry data (OBD-II style)."""
        # Engine RPM based on speed
        if vehicle.state == "MOVING":
            base_rpm = 800 + (vehicle.current_speed * 30)
            vehicle.engine_rpm = int(base_rpm + random.uniform(-200, 200))
            vehicle.engine_temp = min(105, 85 + random.uniform(-5, 15))
        else:
            vehicle.engine_rpm = random.randint(0, 900) if vehicle.state == "IDLE" else 0
            vehicle.engine_temp = max(25, vehicle.engine_temp - 0.5)

        return {
            "event_id": f"TEL-{uuid.uuid4().hex[:12].upper()}",
            "vehicle_id": vehicle.vehicle_id,
            "timestamp": self._get_timestamp(),
            "engine_rpm": vehicle.engine_rpm,
            "engine_temp_c": round(vehicle.engine_temp, 1),
            "fuel_level_pct": round(vehicle.fuel_level, 1),
            "battery_voltage": round(random.uniform(12.2, 14.4), 1),
            "odometer_km": round(vehicle.odometer, 1),
            "engine_hours": round(random.uniform(1000, 5000), 1),
            "dtc_codes": [],  # Diagnostic trouble codes
            "oil_pressure_psi": round(random.uniform(25, 65), 1),
            "coolant_temp_c": round(vehicle.engine_temp - random.uniform(0, 10), 1),
        }

    def run(self, duration_seconds: int = None, max_events: int = None):
        """
        Run the vehicle simulator.

        Args:
            duration_seconds: How long to run (None = forever)
            max_events: Max events to generate (None = unlimited)
        """
        if duration_seconds is not None:
            require_positive_int(duration_seconds, "duration_seconds")
        if max_events is not None:
            require_positive_int(max_events, "max_events")

        if not self.connect():
            logger.error("Failed to connect to Kafka, running in dry-run mode")

        self.start_time = time.time()
        telemetry_counter = 0

        logger.info(f"Starting vehicle simulator with {self.num_vehicles} vehicles")

        try:
            while True:
                cycle_start = time.time()

                for vehicle in self.vehicles:
                    try:
                        prev_speed = vehicle.current_speed

                        # State machine
                        if self._should_stop(vehicle):
                            vehicle.state = "STOPPED"
                            vehicle.current_speed = 0
                            vehicle.last_stop_time = datetime.now()

                            # Check if trip completed
                            if vehicle.destination:
                                dist = self._haversine_distance(
                                    vehicle.current_lat,
                                    vehicle.current_lng,
                                    vehicle.destination["lat"],
                                    vehicle.destination["lng"],
                                )
                                if dist < 1:  # Within 1km
                                    vehicle.trip_id = None
                                    vehicle.destination = None
                                    vehicle.state = "IDLE"
                                    logger.debug(f"Vehicle {vehicle.vehicle_id} completed trip")

                        elif self._should_start_moving(vehicle):
                            self._assign_destination(vehicle)

                        # Update position if moving
                        self._update_position(vehicle)

                        # Generate GPS event
                        gps_event = self.generate_event(vehicle)
                        if self.producer:
                            self.send(gps_event, key=vehicle.vehicle_id)

                        # Detect driving events
                        driving_event = self._detect_driving_event(vehicle, prev_speed)
                        if driving_event:
                            alert = {
                                **driving_event,
                                "event_id": f"DRV-{uuid.uuid4().hex[:12].upper()}",
                                "vehicle_id": vehicle.vehicle_id,
                                "driver_id": vehicle.driver_id,
                                "timestamp": self._get_timestamp(),
                                "latitude": gps_event["latitude"],
                                "longitude": gps_event["longitude"],
                            }
                            if self.producer:
                                self.producer.send(
                                    TOPIC_ALERTS, key=vehicle.vehicle_id, value=alert
                                )

                        # Generate telemetry less frequently (every 30 seconds = every 3rd GPS)
                        telemetry_counter += 1
                        if telemetry_counter % 3 == 0:
                            telemetry = self.generate_telemetry(vehicle)
                            if self.producer:
                                self.producer.send(
                                    self.telemetry_topic, key=vehicle.vehicle_id, value=telemetry
                                )
                    except Exception:
                        logger.exception("Failed to process vehicle %s", vehicle.vehicle_id)

                # Log stats periodically
                if self.message_count % 500 == 0:
                    self._log_stats()
                    moving = sum(1 for v in self.vehicles if v.state == "MOVING")
                    logger.info(f"Vehicles moving: {moving}/{self.num_vehicles}")

                # Check termination conditions
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
    """Run the vehicle simulator standalone."""
    import argparse

    parser = argparse.ArgumentParser(description="Vehicle GPS Simulator")
    parser.add_argument("--vehicles", type=int, default=50, help="Number of vehicles")
    parser.add_argument("--kafka", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--duration", type=int, help="Duration in seconds")
    parser.add_argument("--interval", type=float, default=10.0, help="GPS interval in seconds")

    args = parser.parse_args()

    simulator = VehicleSimulator(
        num_vehicles=args.vehicles,
        kafka_bootstrap_servers=args.kafka,
        gps_interval_seconds=args.interval,
    )
    simulator.run(duration_seconds=args.duration)


if __name__ == "__main__":
    main()
