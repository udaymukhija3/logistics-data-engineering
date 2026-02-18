"""
Generate realistic sample data for the Streamlit dashboard.
This allows the dashboard to run standalone without Kafka/Spark infrastructure.
"""

import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

random.seed(42)
np.random.seed(42)

DATA_DIR = Path(__file__).parent.parent / "data" / "sample"

# Indian city hubs
HUBS = [
    {"id": "HUB_DEL_01", "name": "Delhi Hub", "lat": 28.5505, "lng": 77.2506},
    {"id": "HUB_MUM_01", "name": "Mumbai Hub", "lat": 19.0330, "lng": 72.8520},
    {"id": "HUB_BLR_01", "name": "Bangalore Hub", "lat": 13.0100, "lng": 77.5500},
    {"id": "HUB_HYD_01", "name": "Hyderabad Hub", "lat": 17.4400, "lng": 78.3800},
    {"id": "HUB_CHN_01", "name": "Chennai Hub", "lat": 13.0600, "lng": 80.2100},
    {"id": "HUB_KOL_01", "name": "Kolkata Hub", "lat": 22.5726, "lng": 88.3639},
    {"id": "HUB_PUN_01", "name": "Pune Hub", "lat": 18.5204, "lng": 73.8567},
    {"id": "HUB_AMD_01", "name": "Ahmedabad Hub", "lat": 23.0225, "lng": 72.5714},
]

DELIVERY_ZONES = [
    "DEL_Z1",
    "DEL_Z2",
    "DEL_Z3",
    "DEL_Z4",
    "DEL_Z5",
    "MUM_Z1",
    "MUM_Z2",
    "MUM_Z3",
    "MUM_Z4",
    "BLR_Z1",
    "BLR_Z2",
    "BLR_Z3",
    "BLR_Z4",
]

VEHICLE_TYPES = ["TRUCK", "VAN", "BIKE"]
AGENT_VEHICLE_TYPES = ["BIKE", "SCOOTER", "BICYCLE"]


def generate_vehicle_positions(num_vehicles=30, positions_per_vehicle=100):
    """Generate bronze vehicle_positions data."""
    records = []
    base_time = datetime.now() - timedelta(hours=6)

    for v in range(num_vehicles):
        vehicle_id = f"VEH_{v+1:04d}"
        driver_id = f"DRV_{v+1:04d}"
        vehicle_type = random.choice(VEHICLE_TYPES)
        hub = random.choice(HUBS)
        lat = hub["lat"] + np.random.normal(0, 0.5)
        lng = hub["lng"] + np.random.normal(0, 0.5)

        for p in range(positions_per_vehicle):
            timestamp = base_time + timedelta(seconds=p * 10)
            speed = max(0, np.random.normal(45, 20))
            if random.random() < 0.15:
                speed = 0  # stopped

            lat += np.random.normal(0, 0.002) * (1 if speed > 0 else 0)
            lng += np.random.normal(0, 0.002) * (1 if speed > 0 else 0)

            records.append(
                {
                    "event_id": str(uuid.uuid4()),
                    "vehicle_id": vehicle_id,
                    "driver_id": driver_id,
                    "vehicle_type": vehicle_type,
                    "latitude": round(lat, 6),
                    "longitude": round(lng, 6),
                    "speed_kmh": round(max(0, speed), 1),
                    "heading": round(random.uniform(0, 360), 1),
                    "fuel_level": round(random.uniform(20, 95), 1),
                    "engine_rpm": int(speed * 30 + random.randint(600, 900)) if speed > 0 else 0,
                    "engine_temp_c": round(random.uniform(80, 100), 1),
                    "odometer_km": round(random.uniform(10000, 150000), 1),
                    "timestamp": timestamp.isoformat(),
                    "ingested_at": datetime.now().isoformat(),
                }
            )

    df = pd.DataFrame(records)
    out_path = DATA_DIR / "bronze" / "vehicle_positions"
    out_path.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path / "sample_data.parquet", index=False)
    print(f"  vehicle_positions: {len(df)} rows")
    return df


def generate_shipment_events(num_shipments=500):
    """Generate bronze shipment_events data."""
    event_flow = [
        "CREATED",
        "PICKUP_SCHEDULED",
        "PICKED_UP",
        "HUB_ARRIVED",
        "HUB_INSCAN",
        "HUB_SORTED",
        "HUB_OUTSCAN",
        "HUB_DEPARTED",
        "IN_TRANSIT",
        "HUB_ARRIVED",
        "HUB_INSCAN",
        "HUB_SORTED",
        "HUB_OUTSCAN",
        "HUB_DEPARTED",
        "OUT_FOR_DELIVERY",
        "DELIVERED",
    ]

    records = []
    base_time = datetime.now() - timedelta(days=3)

    for s in range(num_shipments):
        shipment_id = f"SHP_{s+1:06d}"
        origin_hub = random.choice(HUBS)
        dest_hub = random.choice([h for h in HUBS if h["id"] != origin_hub["id"]])
        weight = round(random.uniform(0.5, 30), 2)
        sla_hours = random.choice([24, 48, 72, 96])

        # Decide outcome
        outcome_roll = random.random()
        if outcome_roll < 0.78:
            events_to_gen = event_flow[:]
        elif outcome_roll < 0.88:
            events_to_gen = event_flow[: random.randint(8, 14)] + [
                "DELIVERY_ATTEMPTED",
                "DELIVERED",
            ]
        elif outcome_roll < 0.95:
            events_to_gen = event_flow[: random.randint(8, 14)] + [
                "DELIVERY_ATTEMPTED",
                "DELIVERY_FAILED",
            ]
        else:
            events_to_gen = event_flow[: random.randint(5, 10)]  # still in transit

        event_time = base_time + timedelta(hours=random.uniform(0, 48))

        for event_type in events_to_gen:
            hub = origin_hub if "HUB" not in event_type else random.choice(HUBS)
            records.append(
                {
                    "event_id": str(uuid.uuid4()),
                    "shipment_id": shipment_id,
                    "event_type": event_type,
                    "hub_id": hub["id"],
                    "hub_name": hub["name"],
                    "latitude": hub["lat"] + random.uniform(-0.01, 0.01),
                    "longitude": hub["lng"] + random.uniform(-0.01, 0.01),
                    "weight_kg": weight,
                    "sla_hours": sla_hours,
                    "timestamp": event_time.isoformat(),
                    "ingested_at": datetime.now().isoformat(),
                }
            )
            event_time += timedelta(hours=random.uniform(0.5, 6))

    df = pd.DataFrame(records)
    out_path = DATA_DIR / "bronze" / "shipment_events"
    out_path.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path / "sample_data.parquet", index=False)
    print(f"  shipment_events: {len(df)} rows")
    return df


def generate_delivery_events(num_agents=60, deliveries_per_agent=15):
    """Generate bronze delivery_events data."""
    records = []
    base_time = datetime.now() - timedelta(hours=10)

    for a in range(num_agents):
        agent_id = f"AGT_{a+1:04d}"
        zone_id = random.choice(DELIVERY_ZONES)
        city_prefix = zone_id.split("_")[0]

        hub = next((h for h in HUBS if city_prefix in h["id"]), HUBS[0])
        base_lat = hub["lat"] + random.uniform(-0.1, 0.1)
        base_lng = hub["lng"] + random.uniform(-0.1, 0.1)

        for d in range(deliveries_per_agent):
            event_time = base_time + timedelta(minutes=d * random.randint(15, 40))

            # Decide delivery outcome
            roll = random.random()
            if roll < 0.82:
                event_type = "DELIVERED"
                rating = random.choices([5, 4, 3, 2, 1], weights=[40, 30, 15, 10, 5])[0]
            elif roll < 0.92:
                event_type = "DELIVERY_ATTEMPTED"
                rating = None
            else:
                event_type = "DELIVERY_FAILED"
                rating = None

            records.append(
                {
                    "event_id": str(uuid.uuid4()),
                    "agent_id": agent_id,
                    "order_id": f"ORD_{uuid.uuid4().hex[:8].upper()}",
                    "shipment_id": f"SHP_{random.randint(1, 500):06d}",
                    "event_type": event_type,
                    "zone_id": zone_id,
                    "latitude": round(base_lat + random.uniform(-0.05, 0.05), 6),
                    "longitude": round(base_lng + random.uniform(-0.05, 0.05), 6),
                    "customer_rating": rating,
                    "is_cod": random.random() < 0.3,
                    "cod_amount": (
                        round(random.uniform(200, 5000), 2) if random.random() < 0.3 else 0
                    ),
                    "timestamp": event_time.isoformat(),
                    "ingested_at": datetime.now().isoformat(),
                }
            )

    df = pd.DataFrame(records)
    out_path = DATA_DIR / "bronze" / "delivery_events"
    out_path.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path / "sample_data.parquet", index=False)
    print(f"  delivery_events: {len(df)} rows")
    return df


def generate_silver_trips(vehicle_df):
    """Generate silver trips from vehicle positions."""
    trips = []
    for vehicle_id in vehicle_df["vehicle_id"].unique():
        vdf = vehicle_df[vehicle_df["vehicle_id"] == vehicle_id].sort_values("timestamp")
        num_trips = random.randint(2, 5)

        for t in range(num_trips):
            start_idx = random.randint(0, max(0, len(vdf) - 20))
            trip_positions = vdf.iloc[start_idx : start_idx + random.randint(10, 30)]
            if len(trip_positions) < 5:
                continue

            distance = round(random.uniform(15, 350), 2)
            duration_min = round(distance / random.uniform(30, 60) * 60, 1)

            trips.append(
                {
                    "trip_id": str(uuid.uuid4()),
                    "vehicle_id": vehicle_id,
                    "driver_id": trip_positions.iloc[0]["driver_id"],
                    "vehicle_type": trip_positions.iloc[0]["vehicle_type"],
                    "start_lat": trip_positions.iloc[0]["latitude"],
                    "start_lng": trip_positions.iloc[0]["longitude"],
                    "end_lat": trip_positions.iloc[-1]["latitude"],
                    "end_lng": trip_positions.iloc[-1]["longitude"],
                    "distance_km": distance,
                    "duration_minutes": duration_min,
                    "avg_speed_kmh": round(distance / (duration_min / 60), 1),
                    "max_speed_kmh": round(random.uniform(60, 110), 1),
                    "fuel_consumed_liters": round(distance * random.uniform(0.08, 0.15), 2),
                    "num_positions": len(trip_positions),
                    "num_stops": random.randint(0, 5),
                    "speeding_events": random.randint(0, 3),
                    "harsh_brake_events": random.randint(0, 2),
                    "start_time": trip_positions.iloc[0]["timestamp"],
                    "end_time": trip_positions.iloc[-1]["timestamp"],
                }
            )

    df = pd.DataFrame(trips)
    out_path = DATA_DIR / "silver" / "fleet" / "trips"
    out_path.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path / "sample_data.parquet", index=False)
    print(f"  trips (silver): {len(df)} rows")
    return df


def generate_silver_journeys(shipment_df):
    """Generate silver journeys from shipment events."""
    journeys = []
    for shipment_id in shipment_df["shipment_id"].unique():
        sdf = shipment_df[shipment_df["shipment_id"] == shipment_id].sort_values("timestamp")
        last_event = sdf.iloc[-1]["event_type"]

        if last_event == "DELIVERED":
            outcome = "DELIVERED"
            sla_status = random.choices(["MET", "BREACHED"], weights=[80, 20])[0]
        elif last_event == "DELIVERY_FAILED":
            outcome = "FAILED"
            sla_status = "BREACHED"
        else:
            outcome = "IN_TRANSIT"
            sla_status = random.choices(["ON_TRACK", "AT_RISK"], weights=[70, 30])[0]

        num_hubs = sdf[sdf["event_type"].str.startswith("HUB_")]["hub_id"].nunique()

        journeys.append(
            {
                "journey_id": str(uuid.uuid4()),
                "shipment_id": shipment_id,
                "origin_hub": sdf.iloc[0]["hub_id"],
                "destination_hub": sdf.iloc[-1]["hub_id"],
                "journey_outcome": outcome,
                "sla_status": sla_status,
                "sla_hours": sdf.iloc[0]["sla_hours"],
                "total_events": len(sdf),
                "hubs_traversed": num_hubs,
                "first_event_time": sdf.iloc[0]["timestamp"],
                "last_event_time": sdf.iloc[-1]["timestamp"],
            }
        )

    df = pd.DataFrame(journeys)
    out_path = DATA_DIR / "silver" / "shipment" / "journeys"
    out_path.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path / "sample_data.parquet", index=False)
    print(f"  journeys (silver): {len(df)} rows")
    return df


def generate_silver_agent_shifts(delivery_df):
    """Generate silver agent shift aggregations."""
    shifts = []
    for agent_id in delivery_df["agent_id"].unique():
        adf = delivery_df[delivery_df["agent_id"] == agent_id]
        zone_id = adf.iloc[0]["zone_id"]

        total = len(adf)
        delivered = len(adf[adf["event_type"] == "DELIVERED"])
        attempted = len(adf[adf["event_type"] == "DELIVERY_ATTEMPTED"])
        failed = len(adf[adf["event_type"] == "DELIVERY_FAILED"])

        ratings = adf[adf["customer_rating"].notna()]["customer_rating"]
        avg_rating = round(ratings.mean(), 2) if len(ratings) > 0 else None

        shifts.append(
            {
                "shift_id": str(uuid.uuid4()),
                "agent_id": agent_id,
                "zone_id": zone_id,
                "shift_date": datetime.now().strftime("%Y-%m-%d"),
                "total_deliveries": total,
                "successful_deliveries": delivered,
                "attempted_deliveries": attempted,
                "failed_deliveries": failed,
                "delivery_success_rate": round(delivered / total * 100, 1) if total > 0 else 0,
                "avg_customer_rating": avg_rating,
                "total_distance_km": round(random.uniform(20, 80), 2),
                "active_hours": round(random.uniform(6, 10), 1),
            }
        )

    df = pd.DataFrame(shifts)
    out_path = DATA_DIR / "silver" / "delivery" / "agent_shifts"
    out_path.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path / "sample_data.parquet", index=False)
    print(f"  agent_shifts (silver): {len(df)} rows")
    return df


def generate_quality_report():
    """Generate a sample quality report."""
    import json

    checks = [
        {
            "check": "vehicle_positions_not_null",
            "table": "vehicle_positions",
            "layer": "bronze",
            "success": True,
            "details": "All required fields present",
        },
        {
            "check": "vehicle_speed_range",
            "table": "vehicle_positions",
            "layer": "bronze",
            "success": True,
            "details": "All speeds within 0-200 km/h",
        },
        {
            "check": "coordinates_in_india",
            "table": "vehicle_positions",
            "layer": "bronze",
            "success": True,
            "details": "All coordinates within India bounds",
        },
        {
            "check": "shipment_event_sequence",
            "table": "shipment_events",
            "layer": "bronze",
            "success": True,
            "details": "Event sequences valid",
        },
        {
            "check": "delivery_rating_range",
            "table": "delivery_events",
            "layer": "bronze",
            "success": True,
            "details": "Ratings between 1-5",
        },
        {
            "check": "trip_distance_positive",
            "table": "trips",
            "layer": "silver",
            "success": True,
            "details": "All distances > 0",
        },
        {
            "check": "journey_sla_populated",
            "table": "journeys",
            "layer": "silver",
            "success": True,
            "details": "SLA status set for all journeys",
        },
        {
            "check": "agent_shift_completeness",
            "table": "agent_shifts",
            "layer": "silver",
            "success": False,
            "details": "2 agents missing shift data",
        },
    ]

    report = {
        "timestamp": datetime.now().isoformat(),
        "overall_success": False,
        "summary": {
            "total_checks": len(checks),
            "passed": sum(1 for c in checks if c["success"]),
            "failed": sum(1 for c in checks if not c["success"]),
            "pass_rate": round(sum(1 for c in checks if c["success"]) / len(checks) * 100, 1),
        },
        "checks": checks,
    }

    out_path = DATA_DIR / "quality_reports"
    out_path.mkdir(parents=True, exist_ok=True)
    with open(out_path / "latest_report.json", "w") as f:
        json.dump(report, f, indent=2)
    print(f"  quality_report: {len(checks)} checks")


if __name__ == "__main__":
    print("Generating sample data for Logistics Data Platform...")
    print()

    print("Bronze layer:")
    vehicle_df = generate_vehicle_positions()
    shipment_df = generate_shipment_events()
    delivery_df = generate_delivery_events()

    print()
    print("Silver layer:")
    generate_silver_trips(vehicle_df)
    generate_silver_journeys(shipment_df)
    generate_silver_agent_shifts(delivery_df)

    print()
    print("Quality reports:")
    generate_quality_report()

    print()
    print("Done! Sample data generated in data/ directory.")
