"""
Generate canonical sample datasets for the logistics platform.

The generated data intentionally mirrors the project's Bronze and Silver
contracts so the dashboard, quality checks, and dbt models can all run
against the same sample bundle.
"""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.domain.constants import DELIVERY_FAILURE_REASONS
from src.quality.quality_checks import run_quality_checks

SEED = 42
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "sample"

VEHICLE_TYPES = ("TRUCK", "VAN", "BIKE")
AGENT_VEHICLE_TYPES = ("BIKE", "SCOOTER", "BICYCLE")
ALERT_TYPES = ("SPEEDING", "HARSH_BRAKING", "HARSH_ACCELERATION")
DELIVERY_TIME_SLOTS = ("MORNING", "AFTERNOON", "EVENING")


@dataclass(frozen=True)
class Hub:
    hub_id: str
    name: str
    city: str
    lat: float
    lng: float
    hub_type: str


HUBS: tuple[Hub, ...] = (
    Hub("HUB_DEL_01", "Delhi Hub", "Delhi", 28.6139, 77.2090, "MEGA"),
    Hub("HUB_MUM_01", "Mumbai Hub", "Mumbai", 19.0760, 72.8777, "MEGA"),
    Hub("HUB_BLR_01", "Bangalore Hub", "Bengaluru", 12.9716, 77.5946, "MEGA"),
    Hub("HUB_HYD_01", "Hyderabad Hub", "Hyderabad", 17.3850, 78.4867, "MEGA"),
    Hub("HUB_CHN_01", "Chennai Hub", "Chennai", 13.0827, 80.2707, "MEGA"),
    Hub("HUB_KOL_01", "Kolkata Hub", "Kolkata", 22.5726, 88.3639, "MEGA"),
    Hub("HUB_PUN_01", "Pune Hub", "Pune", 18.5204, 73.8567, "REGIONAL"),
    Hub("HUB_AMD_01", "Ahmedabad Hub", "Ahmedabad", 23.0225, 72.5714, "REGIONAL"),
)

DELIVERY_ZONES = (
    "DEL_Z1",
    "DEL_Z2",
    "DEL_Z3",
    "MUM_Z1",
    "MUM_Z2",
    "MUM_Z3",
    "BLR_Z1",
    "BLR_Z2",
    "BLR_Z3",
    "HYD_Z1",
    "CHN_Z1",
    "KOL_Z1",
)


def seed_random_generators(seed: int = SEED) -> None:
    random.seed(seed)
    np.random.seed(seed)


def _anchor_time() -> datetime:
    return datetime.now(UTC).replace(minute=0, second=0, microsecond=0, tzinfo=None)


def _prepare_output_dir(base_dir: Path) -> None:
    for relative in ("bronze", "silver", "quality_reports"):
        target = base_dir / relative
        if target.exists():
            for child in sorted(target.rglob("*"), reverse=True):
                if child.is_file():
                    child.unlink()
                else:
                    child.rmdir()
        target.mkdir(parents=True, exist_ok=True)


def _write_parquet(df: pd.DataFrame, output_dir: Path, relative_path: str) -> Path:
    path = output_dir / relative_path
    path.mkdir(parents=True, exist_ok=True)
    file_path = path / "sample_data.parquet"
    df.to_parquet(file_path, index=False)
    return file_path


def _write_json(payload: dict[str, Any], file_path: Path) -> None:
    def _json_default(value: Any) -> Any:
        if isinstance(value, (np.bool_,)):
            return bool(value)
        if isinstance(value, (np.integer,)):
            return int(value)
        if isinstance(value, (np.floating,)):
            return float(value)
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")

    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(
        json.dumps(payload, indent=2, default=_json_default),
        encoding="utf-8",
    )


def _hub_for_zone(zone_id: str) -> Hub:
    city_code = zone_id.split("_", 1)[0]
    for hub in HUBS:
        if city_code in hub.hub_id:
            return hub
    return HUBS[0]


def _order_time_slot(timestamp: datetime) -> str:
    hour = timestamp.hour
    if hour < 12:
        return "MORNING"
    if hour < 17:
        return "AFTERNOON"
    return "EVENING"


def generate_vehicle_positions(
    output_dir: Path,
    num_vehicles: int = 30,
    positions_per_vehicle: int = 96,
) -> pd.DataFrame:
    """Generate Bronze vehicle position records with canonical columns."""
    records: list[dict[str, Any]] = []
    base_time = _anchor_time() - timedelta(hours=12)

    for vehicle_index in range(num_vehicles):
        vehicle_id = f"VEH_{vehicle_index + 1:04d}"
        driver_id = f"DRV_{vehicle_index + 1:04d}"
        vehicle_type = random.choice(VEHICLE_TYPES)
        hub = random.choice(HUBS)
        latitude = hub.lat + np.random.normal(0, 0.08)
        longitude = hub.lng + np.random.normal(0, 0.08)
        fuel_level = random.uniform(60, 95)
        odometer_km = random.uniform(8_000, 75_000)
        trip_sequence = 1
        current_time = base_time + timedelta(minutes=random.randint(0, 120))
        trip_id = f"{vehicle_id}_TRIP_{trip_sequence:03d}"

        for position_index in range(positions_per_vehicle):
            if position_index and position_index % 32 == 0:
                current_time += timedelta(minutes=45)
                trip_sequence += 1
                trip_id = f"{vehicle_id}_TRIP_{trip_sequence:03d}"

            current_time += timedelta(minutes=2)
            speed_roll = random.random()
            if speed_roll < 0.12:
                speed_kmh = 0.0
                state = "STOPPED"
            elif speed_roll < 0.22:
                speed_kmh = round(random.uniform(1.0, 4.5), 1)
                state = "IDLE"
            else:
                speed_kmh = round(max(8.0, np.random.normal(46, 14)), 1)
                state = "MOVING"

            interval_hours = 2 / 60
            if state == "MOVING":
                distance_step = speed_kmh * interval_hours
                latitude += np.random.normal(0.0, 0.006)
                longitude += np.random.normal(0.0, 0.006)
                odometer_km += distance_step
                fuel_level = max(5.0, fuel_level - random.uniform(0.02, 0.12))
            else:
                fuel_level = max(5.0, fuel_level - random.uniform(0.0, 0.02))

            records.append(
                {
                    "event_id": f"vp_{uuid.uuid4().hex}",
                    "vehicle_id": vehicle_id,
                    "driver_id": driver_id,
                    "vehicle_type": vehicle_type,
                    "timestamp": current_time.isoformat(),
                    "latitude": round(latitude, 6),
                    "longitude": round(longitude, 6),
                    "speed_kmh": speed_kmh,
                    "heading": round(random.uniform(0, 360), 1),
                    "altitude_m": int(random.uniform(20, 450)),
                    "accuracy_m": round(random.uniform(3, 12), 1),
                    "trip_id": trip_id,
                    "state": state,
                    "fuel_level_pct": round(fuel_level, 2),
                    "odometer_km": round(odometer_km, 2),
                }
            )

    vehicle_positions = pd.DataFrame(records).sort_values(["vehicle_id", "timestamp"])
    _write_parquet(vehicle_positions, output_dir, "bronze/vehicle_positions")
    return vehicle_positions.reset_index(drop=True)


def generate_vehicle_telemetry(output_dir: Path, vehicle_positions: pd.DataFrame) -> pd.DataFrame:
    """Generate Bronze vehicle telemetry records derived from positions."""
    telemetry_records: list[dict[str, Any]] = []

    sampled_positions = vehicle_positions.iloc[::3].reset_index(drop=True)
    for _, row in sampled_positions.iterrows():
        speed = float(row["speed_kmh"])
        telemetry_records.append(
            {
                "event_id": f"vt_{uuid.uuid4().hex}",
                "vehicle_id": row["vehicle_id"],
                "timestamp": row["timestamp"],
                "engine_rpm": int(700 if speed < 2 else min(3800, speed * 45 + random.randint(850, 1200))),
                "engine_temp_c": round(random.uniform(82, 102), 1),
                "fuel_level_pct": row["fuel_level_pct"],
                "battery_voltage": round(random.uniform(12.2, 14.4), 2),
                "odometer_km": row["odometer_km"],
                "engine_hours": round(random.uniform(500, 4500), 1),
                "oil_pressure_psi": round(random.uniform(28, 64), 1),
                "coolant_temp_c": round(random.uniform(78, 105), 1),
            }
        )

    telemetry = pd.DataFrame(telemetry_records)
    _write_parquet(telemetry, output_dir, "bronze/vehicle_telemetry")
    return telemetry


def generate_alerts(output_dir: Path, vehicle_positions: pd.DataFrame) -> pd.DataFrame:
    """Generate Bronze alert records based on vehicle behavior."""
    alerts: list[dict[str, Any]] = []
    previous_speed: dict[str, float] = {}

    for _, row in vehicle_positions.iterrows():
        vehicle_id = row["vehicle_id"]
        driver_id = row["driver_id"]
        speed = float(row["speed_kmh"])
        previous = previous_speed.get(vehicle_id, speed)
        acceleration = speed - previous
        previous_speed[vehicle_id] = speed

        event_type = None
        severity = None
        speed_limit = None
        overspeed_by = None
        deceleration_ms2 = None
        acceleration_ms2 = None

        if speed > 84 and random.random() < 0.35:
            event_type = "SPEEDING"
            speed_limit = random.choice((60, 70, 80))
            overspeed_by = round(max(speed - speed_limit, 0), 1)
            severity = "CRITICAL" if overspeed_by > 25 else "HIGH" if overspeed_by > 15 else "WARNING"
        elif acceleration <= -28 and random.random() < 0.6:
            event_type = "HARSH_BRAKING"
            deceleration_ms2 = round(abs(acceleration) / 3.6, 2)
            severity = "HIGH" if deceleration_ms2 > 8 else "WARNING"
        elif acceleration >= 24 and random.random() < 0.5:
            event_type = "HARSH_ACCELERATION"
            acceleration_ms2 = round(acceleration / 3.6, 2)
            severity = "HIGH" if acceleration_ms2 > 7 else "WARNING"

        if event_type is None:
            continue

        alerts.append(
            {
                "event_id": f"al_{uuid.uuid4().hex}",
                "event_type": event_type,
                "severity": severity or "INFO",
                "timestamp": row["timestamp"],
                "vehicle_id": vehicle_id,
                "driver_id": driver_id,
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "speed": speed,
                "speed_limit": speed_limit,
                "overspeed_by": overspeed_by,
                "deceleration_ms2": deceleration_ms2,
                "acceleration_ms2": acceleration_ms2,
            }
        )

    if not alerts:
        sampled = vehicle_positions.head(5)
        for _, row in sampled.iterrows():
            alerts.append(
                {
                    "event_id": f"al_{uuid.uuid4().hex}",
                    "event_type": random.choice(ALERT_TYPES),
                    "severity": "WARNING",
                    "timestamp": row["timestamp"],
                    "vehicle_id": row["vehicle_id"],
                    "driver_id": row["driver_id"],
                    "latitude": row["latitude"],
                    "longitude": row["longitude"],
                    "speed": row["speed_kmh"],
                    "speed_limit": 70,
                    "overspeed_by": round(max(float(row["speed_kmh"]) - 70, 0), 1),
                    "deceleration_ms2": None,
                    "acceleration_ms2": None,
                }
            )

    alerts_df = pd.DataFrame(alerts)
    _write_parquet(alerts_df, output_dir, "bronze/alerts")
    return alerts_df


def generate_shipment_events(output_dir: Path, num_shipments: int = 500) -> pd.DataFrame:
    """Generate Bronze shipment lifecycle events."""
    records: list[dict[str, Any]] = []
    base_time = _anchor_time() - timedelta(days=4)

    for shipment_index in range(num_shipments):
        shipment_id = f"SHP_{shipment_index + 1:06d}"
        awb_number = f"AWB{shipment_index + 1:010d}"
        seller_id = f"SEL_{random.randint(1, 75):04d}"
        customer_id = f"CUS_{random.randint(1, 5000):05d}"
        origin = random.choice(HUBS)
        destination = random.choice([hub for hub in HUBS if hub.hub_id != origin.hub_id])
        intermediary_count = random.choice((0, 1, 1, 2))
        intermediaries = random.sample(
            [hub for hub in HUBS if hub.hub_id not in {origin.hub_id, destination.hub_id}],
            k=intermediary_count,
        )
        route = [origin, *intermediaries, destination]
        route_hops = len(route)
        weight_kg = round(random.uniform(0.5, 30), 2)
        is_cod = random.random() < 0.28
        cod_amount = round(random.uniform(250, 6500), 2) if is_cod else 0.0
        promised_delivery = base_time + timedelta(
            hours=random.uniform(36, 96),
            minutes=random.randint(0, 59),
        )
        current_time = base_time + timedelta(hours=random.uniform(0, 24))
        delivery_attempts = 0

        lifecycle: list[tuple[str, Hub, int]] = [
            ("CREATED", origin, 0),
            ("PICKUP_SCHEDULED", origin, 0),
            ("PICKED_UP", origin, 0),
        ]
        for hop_index, hub in enumerate(route, start=1):
            lifecycle.extend(
                [
                    ("HUB_ARRIVED", hub, hop_index),
                    ("HUB_INSCAN", hub, hop_index),
                    ("HUB_SORTED", hub, hop_index),
                    ("HUB_OUTSCAN", hub, hop_index),
                    ("HUB_DEPARTED", hub, hop_index),
                ]
            )
            if hop_index < len(route):
                lifecycle.append(("IN_TRANSIT", hub, hop_index))

        lifecycle.append(("OUT_FOR_DELIVERY", destination, route_hops))

        outcome_roll = random.random()
        if outcome_roll < 0.76:
            lifecycle.append(("DELIVERED", destination, route_hops))
        elif outcome_roll < 0.9:
            lifecycle.extend(
                [
                    ("DELIVERY_ATTEMPTED", destination, route_hops),
                    ("DELIVERED", destination, route_hops),
                ]
            )
        elif outcome_roll < 0.97:
            lifecycle.extend(
                [
                    ("DELIVERY_ATTEMPTED", destination, route_hops),
                    ("DELIVERY_FAILED", destination, route_hops),
                ]
            )
        else:
            lifecycle.append(("DELIVERY_ATTEMPTED", destination, route_hops))

        for event_type, hub, current_hop in lifecycle:
            failure_reason = None
            if event_type in {"DELIVERY_ATTEMPTED", "DELIVERY_FAILED"}:
                delivery_attempts += 1
                failure_reason = random.choice(DELIVERY_FAILURE_REASONS)

            records.append(
                {
                    "event_id": f"se_{uuid.uuid4().hex}",
                    "shipment_id": shipment_id,
                    "awb_number": awb_number,
                    "timestamp": current_time.isoformat(),
                    "event_type": event_type,
                    "hub_id": hub.hub_id,
                    "hub_name": hub.name,
                    "hub_city": hub.city,
                    "latitude": round(hub.lat + random.uniform(-0.02, 0.02), 6),
                    "longitude": round(hub.lng + random.uniform(-0.02, 0.02), 6),
                    "seller_id": seller_id,
                    "customer_id": customer_id,
                    "origin_hub": origin.hub_id,
                    "destination_hub": destination.hub_id,
                    "weight_kg": weight_kg,
                    "is_cod": is_cod,
                    "cod_amount": cod_amount,
                    "promised_delivery": promised_delivery.isoformat(),
                    "route_hops": route_hops,
                    "current_hop": current_hop,
                    "delivery_attempts": delivery_attempts,
                    "failure_reason": failure_reason,
                    "scanner_id": f"SCN_{random.randint(100, 999)}",
                    "worker_id": f"WRK_{random.randint(1000, 9999)}",
                }
            )
            current_time += timedelta(hours=random.uniform(1.0, 8.0), minutes=random.randint(0, 50))

    shipment_events = pd.DataFrame(records).sort_values(["shipment_id", "timestamp"])
    _write_parquet(shipment_events, output_dir, "bronze/shipment_events")
    return shipment_events.reset_index(drop=True)


def generate_delivery_events(
    output_dir: Path,
    num_agents: int = 60,
    deliveries_per_agent: int = 18,
) -> pd.DataFrame:
    """Generate Bronze delivery events with canonical columns."""
    records: list[dict[str, Any]] = []
    base_time = _anchor_time() - timedelta(hours=10)

    for agent_index in range(num_agents):
        agent_id = f"AGT_{agent_index + 1:04d}"
        agent_name = f"Agent {agent_index + 1:03d}"
        zone_id = random.choice(DELIVERY_ZONES)
        hub = _hub_for_zone(zone_id)
        vehicle_type = random.choice(AGENT_VEHICLE_TYPES)
        current_time = base_time + timedelta(minutes=random.randint(0, 90))

        for delivery_index in range(deliveries_per_agent):
            order_id = f"ORD_{uuid.uuid4().hex[:10].upper()}"
            shipment_id = f"SHP_{random.randint(1, 500):06d}"
            customer_id = f"CUS_{random.randint(1, 5000):05d}"
            delivery_lat = round(hub.lat + random.uniform(-0.05, 0.05), 6)
            delivery_lng = round(hub.lng + random.uniform(-0.05, 0.05), 6)
            is_cod = random.random() < 0.3
            cod_amount = round(random.uniform(200, 4200), 2) if is_cod else 0.0
            failure_reason = None

            outcome_roll = random.random()
            if outcome_roll < 0.78:
                lifecycle = [("DELIVERED", 1)]
            elif outcome_roll < 0.92:
                lifecycle = [("DELIVERY_ATTEMPTED", 1), ("DELIVERED", 2)]
            else:
                lifecycle = [("DELIVERY_ATTEMPTED", 1), ("DELIVERY_FAILED", 2)]

            for event_type, attempt_number in lifecycle:
                timestamp = current_time + timedelta(minutes=attempt_number * random.randint(12, 35))
                if event_type != "DELIVERED":
                    failure_reason = random.choice(DELIVERY_FAILURE_REASONS)

                rating = None
                cod_collected = 0.0
                pod_type = None
                if event_type == "DELIVERED":
                    rating = random.choices([5, 4, 3, 2, 1], weights=[42, 28, 16, 9, 5])[0]
                    cod_collected = cod_amount if is_cod else 0.0
                    pod_type = random.choice(("OTP", "PHOTO", "SIGNATURE"))

                records.append(
                    {
                        "event_id": f"de_{uuid.uuid4().hex}",
                        "event_type": event_type,
                        "timestamp": timestamp.isoformat(),
                        "agent_id": agent_id,
                        "agent_name": agent_name,
                        "order_id": order_id,
                        "shipment_id": shipment_id,
                        "customer_id": customer_id,
                        "delivery_lat": delivery_lat,
                        "delivery_lng": delivery_lng,
                        "delivery_address": f"{random.randint(10, 999)} {hub.city} Road, {hub.city}",
                        "zone_id": zone_id,
                        "is_cod": is_cod,
                        "cod_amount": cod_amount,
                        "cod_collected": cod_collected,
                        "payment_mode": "COD" if is_cod else random.choice(("UPI", "CARD", "PREPAID")),
                        "attempt_number": attempt_number,
                        "failure_reason": failure_reason,
                        "pod_type": pod_type,
                        "customer_rating": rating,
                        "time_at_location_seconds": random.randint(60, 720),
                        "vehicle_type": vehicle_type,
                    }
                )

            current_time += timedelta(minutes=random.randint(20, 45))

    delivery_events = pd.DataFrame(records).sort_values(["agent_id", "timestamp"])
    _write_parquet(delivery_events, output_dir, "bronze/delivery_events")
    return delivery_events.reset_index(drop=True)


def generate_agent_positions(output_dir: Path, delivery_events: pd.DataFrame) -> pd.DataFrame:
    """Generate Bronze agent position records around delivery activity."""
    positions: list[dict[str, Any]] = []

    completed_today: dict[str, int] = {}
    failed_today: dict[str, int] = {}
    pending_orders: dict[str, int] = {}

    for agent_id, agent_events in delivery_events.groupby("agent_id"):
        agent_events = agent_events.sort_values("timestamp").reset_index(drop=True)
        zone_id = agent_events.loc[0, "zone_id"]
        hub = _hub_for_zone(zone_id)
        vehicle_type = agent_events.loc[0, "vehicle_type"]
        battery_pct = random.uniform(55, 96)
        completed_today[agent_id] = 0
        failed_today[agent_id] = 0
        pending_orders[agent_id] = int(agent_events["order_id"].nunique())

        for _, event in agent_events.iterrows():
            event_time = pd.Timestamp(event["timestamp"]).to_pydatetime()
            current_order = event["order_id"]
            samples = (
                (event_time - timedelta(minutes=12), random.uniform(8, 22), False, "ON_DELIVERY"),
                (event_time - timedelta(minutes=4), random.uniform(4, 12), False, "AVAILABLE"),
                (event_time + timedelta(minutes=1), random.uniform(0, 1.5), True, "BREAK"),
            )
            for sample_time, speed, is_at_stop, status in samples:
                positions.append(
                    {
                        "event_id": f"ap_{uuid.uuid4().hex}",
                        "agent_id": agent_id,
                        "timestamp": sample_time.isoformat(),
                        "latitude": round(float(event["delivery_lat"]) + random.uniform(-0.01, 0.01), 6),
                        "longitude": round(float(event["delivery_lng"]) + random.uniform(-0.01, 0.01), 6),
                        "speed_kmh": round(speed, 1),
                        "heading": round(random.uniform(0, 360), 1),
                        "accuracy_m": round(random.uniform(4, 15), 1),
                        "zone_id": zone_id,
                        "vehicle_type": vehicle_type,
                        "status": status,
                        "is_at_stop": is_at_stop,
                        "current_order_id": current_order,
                        "pending_orders": pending_orders[agent_id],
                        "completed_today": completed_today[agent_id],
                        "failed_today": failed_today[agent_id],
                        "battery_pct": round(battery_pct, 1),
                    }
                )
                battery_pct = max(12.0, battery_pct - random.uniform(0.1, 0.4))

            if event["event_type"] == "DELIVERED":
                completed_today[agent_id] += 1
                pending_orders[agent_id] = max(0, pending_orders[agent_id] - 1)
            elif event["event_type"] == "DELIVERY_FAILED":
                failed_today[agent_id] += 1
                pending_orders[agent_id] = max(0, pending_orders[agent_id] - 1)

        # Add a depot return point to extend the shift.
        last_event_time = pd.Timestamp(agent_events.iloc[-1]["timestamp"]).to_pydatetime() + timedelta(minutes=25)
        positions.append(
            {
                "event_id": f"ap_{uuid.uuid4().hex}",
                "agent_id": agent_id,
                "timestamp": last_event_time.isoformat(),
                "latitude": round(hub.lat + random.uniform(-0.01, 0.01), 6),
                "longitude": round(hub.lng + random.uniform(-0.01, 0.01), 6),
                "speed_kmh": 0.0,
                "heading": round(random.uniform(0, 360), 1),
                "accuracy_m": round(random.uniform(4, 15), 1),
                "zone_id": zone_id,
                "vehicle_type": vehicle_type,
                "status": "OFFLINE",
                "is_at_stop": True,
                "current_order_id": None,
                "pending_orders": pending_orders[agent_id],
                "completed_today": completed_today[agent_id],
                "failed_today": failed_today[agent_id],
                "battery_pct": round(battery_pct, 1),
            }
        )

    agent_positions = pd.DataFrame(positions).sort_values(["agent_id", "timestamp"])
    _write_parquet(agent_positions, output_dir, "bronze/agent_positions")
    return agent_positions.reset_index(drop=True)


def generate_silver_trips(output_dir: Path, vehicle_positions: pd.DataFrame) -> pd.DataFrame:
    """Generate Silver trip summaries from canonical position data."""
    vehicle_positions = vehicle_positions.copy()
    vehicle_positions["timestamp"] = pd.to_datetime(vehicle_positions["timestamp"])

    trips: list[dict[str, Any]] = []
    for (vehicle_id, trip_id), trip_df in vehicle_positions.groupby(["vehicle_id", "trip_id"]):
        trip_df = trip_df.sort_values("timestamp").reset_index(drop=True)
        if len(trip_df) < 5:
            continue

        start_row = trip_df.iloc[0]
        end_row = trip_df.iloc[-1]
        total_distance_km = round(float(end_row["odometer_km"]) - float(start_row["odometer_km"]), 2)
        if total_distance_km <= 0:
            total_distance_km = round(float(trip_df["speed_kmh"].mean()) * (len(trip_df) * 2 / 60), 2)

        duration_minutes = round((end_row["timestamp"] - start_row["timestamp"]).total_seconds() / 60, 1)
        straight_line_distance = round(
            111.0
            * (
                (float(end_row["latitude"]) - float(start_row["latitude"])) ** 2
                + (
                    (float(end_row["longitude"]) - float(start_row["longitude"]))
                    * np.cos(np.radians(float(start_row["latitude"])))
                )
                ** 2
            )
            ** 0.5,
            2,
        )

        if straight_line_distance < 5:
            trip_type = "LOCAL"
        elif straight_line_distance < 50:
            trip_type = "SHORT_HAUL"
        elif straight_line_distance < 200:
            trip_type = "MEDIUM_HAUL"
        else:
            trip_type = "LONG_HAUL"

        fuel_start_pct = round(float(trip_df["fuel_level_pct"].iloc[0]), 2)
        fuel_end_pct = round(float(trip_df["fuel_level_pct"].iloc[-1]), 2)
        fuel_consumed_pct = round(max(fuel_start_pct - fuel_end_pct, 0), 2)

        trips.append(
            {
                "trip_id": trip_id,
                "vehicle_id": vehicle_id,
                "driver_id": start_row["driver_id"],
                "vehicle_type": start_row["vehicle_type"],
                "trip_start_time": start_row["timestamp"].isoformat(),
                "trip_end_time": end_row["timestamp"].isoformat(),
                "trip_date": start_row["timestamp"].date().isoformat(),
                "start_latitude": float(start_row["latitude"]),
                "start_longitude": float(start_row["longitude"]),
                "end_latitude": float(end_row["latitude"]),
                "end_longitude": float(end_row["longitude"]),
                "total_distance_km": total_distance_km,
                "position_count": int(len(trip_df)),
                "trip_duration_minutes": duration_minutes,
                "avg_speed_kmh": round(float(trip_df["speed_kmh"].mean()), 1),
                "max_speed_kmh": round(float(trip_df["speed_kmh"].max()), 1),
                "fuel_start_pct": fuel_start_pct,
                "fuel_end_pct": fuel_end_pct,
                "fuel_consumed_pct": fuel_consumed_pct,
                "stop_count": int((trip_df["state"] == "STOPPED").sum()),
                "straight_line_distance_km": straight_line_distance,
                "route_efficiency": round(
                    straight_line_distance / total_distance_km, 3
                )
                if total_distance_km > 0
                else None,
                "trip_type": trip_type,
                "is_round_trip": straight_line_distance < 2.0,
            }
        )

    trips_df = pd.DataFrame(trips).sort_values(["vehicle_id", "trip_start_time"])
    _write_parquet(trips_df, output_dir, "silver/fleet/trips")
    return trips_df.reset_index(drop=True)


def generate_silver_journeys(output_dir: Path, shipment_events: pd.DataFrame) -> pd.DataFrame:
    """Generate Silver shipment journey summaries."""
    shipment_events = shipment_events.copy()
    shipment_events["timestamp"] = pd.to_datetime(shipment_events["timestamp"])

    journeys: list[dict[str, Any]] = []
    for shipment_id, journey_df in shipment_events.groupby("shipment_id"):
        journey_df = journey_df.sort_values("timestamp").reset_index(drop=True)
        first_row = journey_df.iloc[0]
        last_row = journey_df.iloc[-1]
        promised_delivery = pd.Timestamp(first_row["promised_delivery"])
        end_time = pd.Timestamp(last_row["timestamp"])
        duration_hours = round((end_time - pd.Timestamp(first_row["timestamp"])).total_seconds() / 3600, 2)
        last_event = last_row["event_type"]

        if last_event == "DELIVERED":
            journey_outcome = "DELIVERED"
            sla_status = "MET" if end_time <= promised_delivery else "BREACHED"
        elif last_event == "DELIVERY_FAILED":
            journey_outcome = "FAILED"
            sla_status = "FAILED"
        elif last_event == "DELIVERY_ATTEMPTED":
            journey_outcome = "AT_RISK"
            sla_status = "AT_RISK"
        else:
            journey_outcome = "IN_TRANSIT"
            sla_status = "ON_TRACK"

        gaps = journey_df["timestamp"].diff().dt.total_seconds().div(3600).fillna(0)
        journeys.append(
            {
                "journey_id": f"JNY_{uuid.uuid4().hex[:12].upper()}",
                "shipment_id": shipment_id,
                "origin_hub": first_row["origin_hub"],
                "destination_hub": first_row["destination_hub"],
                "journey_start_time": pd.Timestamp(first_row["timestamp"]).isoformat(),
                "journey_end_time": end_time.isoformat(),
                "journey_date": pd.Timestamp(first_row["timestamp"]).date().isoformat(),
                "journey_duration_hours": duration_hours,
                "journey_outcome": journey_outcome,
                "sla_status": sla_status,
                "sla_variance_hours": round((end_time - promised_delivery).total_seconds() / 3600, 2)
                if last_event == "DELIVERED"
                else None,
                "total_events": int(len(journey_df)),
                "hubs_traversed": int(journey_df["hub_id"].nunique()),
                "stuck_incidents": int((gaps > 24).sum()),
                "delivery_attempts": int(journey_df["delivery_attempts"].max()),
            }
        )

    journeys_df = pd.DataFrame(journeys).sort_values(["journey_date", "shipment_id"])
    _write_parquet(journeys_df, output_dir, "silver/shipment/journeys")
    return journeys_df.reset_index(drop=True)


def generate_silver_agent_shifts(
    output_dir: Path,
    agent_positions: pd.DataFrame,
    delivery_events: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate Silver agent and zone summaries."""
    positions = agent_positions.copy()
    positions["timestamp"] = pd.to_datetime(positions["timestamp"])
    positions["event_date"] = positions["timestamp"].dt.date.astype(str)

    deliveries = delivery_events.copy()
    deliveries["timestamp"] = pd.to_datetime(deliveries["timestamp"])
    deliveries["event_date"] = deliveries["timestamp"].dt.date.astype(str)

    shift_rows: list[dict[str, Any]] = []
    zone_rows: list[dict[str, Any]] = []

    grouped = positions.groupby(["agent_id", "event_date"])
    for (agent_id, event_date), position_df in grouped:
        position_df = position_df.sort_values("timestamp").reset_index(drop=True)
        zone_id = position_df.iloc[0]["zone_id"]
        delivery_df = deliveries[
            (deliveries["agent_id"] == agent_id) & (deliveries["event_date"] == event_date)
        ].sort_values("timestamp")
        shift_start = position_df.iloc[0]["timestamp"]
        shift_end = position_df.iloc[-1]["timestamp"]
        shift_duration_hours = round((shift_end - shift_start).total_seconds() / 3600, 2)
        successful = int((delivery_df["event_type"] == "DELIVERED").sum())
        attempts = int((delivery_df["event_type"] == "DELIVERY_ATTEMPTED").sum())
        final_failures = int((delivery_df["event_type"] == "DELIVERY_FAILED").sum())
        distance_km = round(
            max(position_df["speed_kmh"].mean(), 5) * shift_duration_hours * 0.45,
            2,
        )
        ratings = delivery_df["customer_rating"].dropna()

        shift_rows.append(
            {
                "shift_id": f"SHF_{uuid.uuid4().hex[:12].upper()}",
                "agent_id": agent_id,
                "zone_id": zone_id,
                "shift_date": event_date,
                "shift_start_time": shift_start.isoformat(),
                "shift_end_time": shift_end.isoformat(),
                "shift_duration_hours": shift_duration_hours,
                "position_count": int(len(position_df)),
                "stop_count": int(position_df["is_at_stop"].sum()),
                "avg_speed_kmh": round(float(position_df["speed_kmh"].mean()), 1),
                "max_speed_kmh": round(float(position_df["speed_kmh"].max()), 1),
                "successful_deliveries": successful,
                "failed_attempts": attempts,
                "final_failures": final_failures,
                "first_attempt_successes": int(
                    len(
                        delivery_df[
                            (delivery_df["event_type"] == "DELIVERED")
                            & (delivery_df["attempt_number"] == 1)
                        ]
                    )
                ),
                "delivery_success_rate": round(
                    successful / max(successful + attempts + final_failures, 1),
                    3,
                ),
                "deliveries_per_hour": round(successful / max(shift_duration_hours, 1), 2),
                "avg_customer_rating": round(float(ratings.mean()), 2) if not ratings.empty else None,
                "ratings_received": int(ratings.count()),
                "total_cod_collected": round(
                    float(delivery_df.loc[delivery_df["event_type"] == "DELIVERED", "cod_collected"].sum()),
                    2,
                ),
                "total_distance_km": distance_km,
                "performance_tier": "TOP_PERFORMER"
                if successful >= 18
                else "GOOD"
                if successful >= 12
                else "AVERAGE"
                if successful >= 6
                else "BELOW_AVERAGE",
            }
        )

    shifts_df = pd.DataFrame(shift_rows).sort_values(["shift_date", "agent_id"])
    _write_parquet(shifts_df, output_dir, "silver/delivery/agent_shifts")

    if not shifts_df.empty:
        for (zone_id, shift_date), zone_df in shifts_df.groupby(["zone_id", "shift_date"]):
            zone_rows.append(
                {
                    "zone_id": zone_id,
                    "event_date": shift_date,
                    "active_agents": int(zone_df["agent_id"].nunique()),
                    "total_deliveries": int(zone_df["successful_deliveries"].sum()),
                    "avg_success_rate": round(float(zone_df["delivery_success_rate"].mean()), 3),
                    "avg_deliveries_per_hour": round(float(zone_df["deliveries_per_hour"].mean()), 2),
                    "avg_customer_rating": round(float(zone_df["avg_customer_rating"].dropna().mean()), 2)
                    if zone_df["avg_customer_rating"].dropna().any()
                    else None,
                    "top_performer_count": int((zone_df["performance_tier"] == "TOP_PERFORMER").sum()),
                }
            )

    zone_df = pd.DataFrame(zone_rows).sort_values(["event_date", "zone_id"])
    _write_parquet(zone_df, output_dir, "silver/delivery/zone_performance")
    return shifts_df.reset_index(drop=True), zone_df.reset_index(drop=True)


def generate_quality_report(output_dir: Path) -> dict[str, Any]:
    """Run real quality checks and persist a stable latest report."""
    success, report = run_quality_checks(
        layer="all",
        data_path=str(output_dir),
        output_path=str(output_dir / "quality_reports"),
        use_spark=False,
    )
    report["sample_bundle_success"] = success
    _write_json(report, output_dir / "quality_reports" / "latest_report.json")
    for historical_report in (output_dir / "quality_reports").glob("quality_all_*.json"):
        historical_report.unlink()
    return report


def generate_manifest(
    output_dir: Path,
    datasets: dict[str, pd.DataFrame],
    quality_report: dict[str, Any],
) -> dict[str, Any]:
    """Write a summary manifest used by docs and the dashboard."""
    manifest = {
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "seed": SEED,
        "datasets": {
            name: {
                "rows": int(len(df)),
                "columns": list(df.columns),
            }
            for name, df in datasets.items()
        },
        "quality": quality_report.get("summary", {}),
    }
    _write_json(manifest, output_dir / "manifest.json")
    return manifest


def build_sample_dataset(output_dir: Path | None = None) -> dict[str, Any]:
    """Generate the full sample dataset bundle."""
    destination = output_dir or DATA_DIR
    destination.mkdir(parents=True, exist_ok=True)
    seed_random_generators()
    _prepare_output_dir(destination)

    vehicle_positions = generate_vehicle_positions(destination)
    vehicle_telemetry = generate_vehicle_telemetry(destination, vehicle_positions)
    alerts = generate_alerts(destination, vehicle_positions)
    shipment_events = generate_shipment_events(destination)
    delivery_events = generate_delivery_events(destination)
    agent_positions = generate_agent_positions(destination, delivery_events)
    trips = generate_silver_trips(destination, vehicle_positions)
    journeys = generate_silver_journeys(destination, shipment_events)
    agent_shifts, zone_performance = generate_silver_agent_shifts(
        destination, agent_positions, delivery_events
    )
    quality_report = generate_quality_report(destination)

    datasets = {
        "bronze.vehicle_positions": vehicle_positions,
        "bronze.vehicle_telemetry": vehicle_telemetry,
        "bronze.shipment_events": shipment_events,
        "bronze.agent_positions": agent_positions,
        "bronze.delivery_events": delivery_events,
        "bronze.alerts": alerts,
        "silver.fleet.trips": trips,
        "silver.shipment.journeys": journeys,
        "silver.delivery.agent_shifts": agent_shifts,
        "silver.delivery.zone_performance": zone_performance,
    }
    manifest = generate_manifest(destination, datasets, quality_report)

    return {
        "output_dir": destination,
        "datasets": datasets,
        "quality_report": quality_report,
        "manifest": manifest,
    }


def main() -> None:
    result = build_sample_dataset()
    summary = result["quality_report"]["summary"]
    print("Generated logistics sample data bundle")
    print(f"Output directory: {result['output_dir']}")
    print(
        f"Quality checks: {summary.get('passed', 0)}/{summary.get('total_checks', 0)} passed"
    )


if __name__ == "__main__":
    main()
