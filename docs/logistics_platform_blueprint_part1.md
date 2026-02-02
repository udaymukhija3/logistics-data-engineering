# UNIFIED LOGISTICS DATA PLATFORM
## Complete Data Engineering Project Blueprint

---

# SECTION 1: EXECUTIVE OVERVIEW

## 1.1 What This Project Is

A comprehensive data platform for logistics operations covering:

| Module | What It Tracks | Key Questions Answered |
|--------|---------------|------------------------|
| **Fleet Telematics** | Vehicles (trucks, vans) | Where are our vehicles? Are drivers safe? |
| **Shipment Tracking** | Packages across hub network | Where is package X? Will it arrive on time? |
| **Last-Mile Delivery** | Delivery agents | How efficient are our delivery partners? |

This mirrors how real logistics companies (Delhivery, Ecom Express, FedEx) operate.

## 1.2 Why Combined is Better Than Separate

| Aspect | Three Separate Projects | One Unified Platform |
|--------|------------------------|----------------------|
| **Realism** | Artificial separation | How companies actually work |
| **Data Model** | Redundant dimensions | Shared dimensions, clear relationships |
| **Infrastructure** | 3x Kafka, 3x Spark | Shared infrastructure |
| **Interview Story** | "I did three logistics projects" | "I built an end-to-end logistics platform" |
| **Complexity** | Simpler individually | More impressive overall |

## 1.3 The Business Context

**Imagine you're building the data platform for Delhivery:**

1. **Seller ships package** → Package enters system
2. **Pickup agent collects** → First-mile tracking
3. **Truck takes to hub** → Fleet telematics + shipment movement
4. **Package scanned at hub** → Shipment events
5. **Truck moves between hubs** → Fleet + shipment correlation
6. **Package reaches destination hub** → Shipment events
7. **Delivery agent assigned** → Last-mile begins
8. **Delivery attempted** → Last-mile analytics
9. **Package delivered** → End-to-end journey complete

**All three modules work together.**

## 1.4 Key Relationships

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ENTITY RELATIONSHIPS                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  VEHICLE ←───────────────── carries ─────────────────→ SHIPMENT             │
│     │                                                      │                 │
│     │ tracked by                                           │ scanned at     │
│     ▼                                                      ▼                 │
│  GPS POSITIONS                                          HUB EVENTS          │
│                                                            │                 │
│                                                            │ assigned to    │
│                                                            ▼                 │
│  DELIVERY AGENT ←────────── delivers ──────────────→ DELIVERY ATTEMPT       │
│     │                                                                        │
│     │ tracked by                                                            │
│     ▼                                                                        │
│  AGENT POSITIONS                                                            │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

# SECTION 2: HIGH-LEVEL ARCHITECTURE

## 2.1 System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │ GPS DEVICES     │  │ SCANNER APPS    │  │ DELIVERY APP    │             │
│  │ (Vehicles)      │  │ (Hub Workers)   │  │ (Agents)        │             │
│  │                 │  │                 │  │                 │             │
│  │ • Location      │  │ • Package scans │  │ • Location      │             │
│  │ • Speed         │  │ • Hub events    │  │ • Deliveries    │             │
│  │ • Telemetry     │  │ • Exceptions    │  │ • POD capture   │             │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘             │
│           │                    │                    │                       │
│           ▼                    ▼                    ▼                       │
│  ┌─────────────────────────────────────────────────────────────┐           │
│  │                    KAFKA CLUSTER                             │           │
│  │                                                              │           │
│  │  Topics:                                                     │           │
│  │  • vehicle_positions      (GPS every 10 sec)                │           │
│  │  • vehicle_telemetry      (OBD data every 30 sec)           │           │
│  │  • shipment_events        (scans, status changes)           │           │
│  │  • agent_positions        (GPS every 30 sec)                │           │
│  │  • delivery_events        (attempts, completions)           │           │
│  │  • alerts                 (all alert types)                 │           │
│  └─────────────────────────────────────────────────────────────┘           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        STREAM PROCESSING                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────┐           │
│  │              SPARK STRUCTURED STREAMING                      │           │
│  │                                                              │           │
│  │  Job 1: Vehicle Position Processing                         │           │
│  │  ├── Validate GPS coordinates                               │           │
│  │  ├── Calculate H3 index                                     │           │
│  │  ├── Detect geofence entry/exit                            │           │
│  │  ├── Detect driving events (speeding, harsh brake)         │           │
│  │  └── Update vehicle current state                          │           │
│  │                                                              │           │
│  │  Job 2: Shipment Event Processing                           │           │
│  │  ├── Validate event sequence                                │           │
│  │  ├── Update shipment current state                         │           │
│  │  ├── Check SLA status                                       │           │
│  │  ├── Detect stuck shipments                                │           │
│  │  └── Correlate with vehicle (which truck carries this?)    │           │
│  │                                                              │           │
│  │  Job 3: Delivery Processing                                 │           │
│  │  ├── Detect stops (from GPS pattern)                       │           │
│  │  ├── Match stops to assigned orders                        │           │
│  │  ├── Calculate time at stop                                │           │
│  │  ├── Process delivery outcomes                             │           │
│  │  └── Update agent metrics                                  │           │
│  │                                                              │           │
│  │  Job 4: Alert Generation                                    │           │
│  │  ├── Aggregate alerts from all modules                     │           │
│  │  ├── Deduplicate                                           │           │
│  │  ├── Route to appropriate channels                         │           │
│  │  └── Write to alerts topic                                 │           │
│  └─────────────────────────────────────────────────────────────┘           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA LAKE                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │     BRONZE      │  │     SILVER      │  │      GOLD       │             │
│  │   (Raw Events)  │  │   (Cleaned)     │  │  (Aggregated)   │             │
│  │                 │  │                 │  │                 │             │
│  │ • Immutable     │  │ • Validated     │  │ • Fact tables   │             │
│  │ • Partitioned   │  │ • Enriched      │  │ • Dim tables    │             │
│  │   by date       │  │ • Deduplicated  │  │ • Aggregates    │             │
│  │ • Parquet       │  │ • Parquet       │  │ • Parquet       │             │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘             │
│                                                                              │
│  Storage: S3 / MinIO (local)                                                │
│  Format: Delta Lake (ACID transactions, time travel)                        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        BATCH PROCESSING                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────┐           │
│  │                    APACHE SPARK (Batch)                      │           │
│  │                                                              │           │
│  │  Daily Jobs:                                                 │           │
│  │  • Trip reconstruction (GPS points → complete trips)        │           │
│  │  • Shipment journey reconstruction                          │           │
│  │  • Agent shift aggregation                                  │           │
│  │  • SLA compliance calculation                               │           │
│  │  • Hub throughput metrics                                   │           │
│  │  • Route performance analytics                              │           │
│  │  • Area-level delivery analytics                            │           │
│  └─────────────────────────────────────────────────────────────┘           │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────┐           │
│  │                          dbt                                 │           │
│  │                                                              │           │
│  │  Transformations:                                            │           │
│  │  • Staging models (one per source)                          │           │
│  │  • Intermediate models (business logic)                     │           │
│  │  • Mart models (analytics-ready facts and dims)            │           │
│  │  • Tests on every model                                     │           │
│  └─────────────────────────────────────────────────────────────┘           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DATA WAREHOUSE                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────┐           │
│  │              DuckDB / ClickHouse / PostgreSQL                │           │
│  │                                                              │           │
│  │  Star Schema:                                                │           │
│  │  • Fact tables (events, metrics)                            │           │
│  │  • Dimension tables (vehicles, shipments, agents, etc.)     │           │
│  │  • Pre-aggregated rollups (hourly, daily)                   │           │
│  │                                                              │           │
│  │  + PostGIS extension for geospatial queries                 │           │
│  └─────────────────────────────────────────────────────────────┘           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATION                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────┐           │
│  │                      APACHE AIRFLOW                          │           │
│  │                                                              │           │
│  │  DAGs:                                                       │           │
│  │  • daily_batch_processing (trips, journeys, aggregations)   │           │
│  │  • hourly_quality_checks (data quality monitoring)          │           │
│  │  • dbt_transformations (staging → intermediate → marts)     │           │
│  │  • sla_reporting (daily SLA compliance reports)             │           │
│  │  • data_retention (archive old data, manage storage)        │           │
│  └─────────────────────────────────────────────────────────────┘           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 2.2 Technology Stack Summary

| Layer | Technology | Why |
|-------|------------|-----|
| **Message Queue** | Apache Kafka | High-throughput, durable, industry standard |
| **Stream Processing** | Spark Structured Streaming | Unified batch + stream, mature, scalable |
| **Batch Processing** | Apache Spark | Handles large-scale transformations |
| **Data Lake** | Delta Lake on S3/MinIO | ACID transactions, time travel, schema evolution |
| **Transformations** | dbt | SQL-based, testable, documented |
| **Warehouse** | DuckDB + PostGIS | Fast analytics + geospatial |
| **Orchestration** | Apache Airflow | Industry standard, rich ecosystem |
| **Data Quality** | Great Expectations | Comprehensive validation framework |
| **Geospatial** | H3, PostGIS | Efficient spatial indexing and queries |
| **Containerization** | Docker Compose | Local development, reproducible |

---

# SECTION 3: DATA SOURCES (SIMULATION)

## 3.1 Why Simulation?

Real logistics data is proprietary. We'll build realistic simulators that generate data matching real-world patterns:

- GPS coordinates along actual roads (using OSRM)
- Realistic shipment volumes and patterns
- Delivery success/failure distributions matching industry norms

## 3.2 Vehicle GPS Simulator

```python
# src/simulators/vehicle_gps_simulator.py

"""
Generates realistic vehicle GPS data:
- Vehicles follow actual road networks (OSRM routing)
- Speed varies by road type, time of day
- Includes noise, occasional GPS drift
- Generates telemetry (fuel, RPM, etc.)
"""

import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
import requests
import h3

class VehicleGPSSimulator:
    def __init__(self, kafka_bootstrap: str, num_vehicles: int = 50):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.num_vehicles = num_vehicles
        self.vehicles = self._initialize_vehicles()
        
    def _initialize_vehicles(self):
        """Create vehicles with random starting positions and routes."""
        vehicles = []
        
        # Major Indian cities as hub locations
        hubs = [
            {"name": "Delhi", "lat": 28.6139, "lng": 77.2090},
            {"name": "Mumbai", "lat": 19.0760, "lng": 72.8777},
            {"name": "Bangalore", "lat": 12.9716, "lng": 77.5946},
            {"name": "Chennai", "lat": 13.0827, "lng": 80.2707},
            {"name": "Hyderabad", "lat": 17.3850, "lng": 78.4867},
            {"name": "Kolkata", "lat": 22.5726, "lng": 88.3639},
            {"name": "Pune", "lat": 18.5204, "lng": 73.8567},
            {"name": "Ahmedabad", "lat": 23.0225, "lng": 72.5714},
        ]
        
        for i in range(self.num_vehicles):
            origin = random.choice(hubs)
            destination = random.choice([h for h in hubs if h != origin])
            
            vehicles.append({
                "vehicle_id": f"VH{i+1:04d}",
                "vehicle_type": random.choice(["TRUCK_20FT", "TRUCK_32FT", "VAN", "PICKUP"]),
                "current_lat": origin["lat"],
                "current_lng": origin["lng"],
                "destination_lat": destination["lat"],
                "destination_lng": destination["lng"],
                "speed_kmh": 0,
                "heading": 0,
                "fuel_level": random.uniform(50, 100),
                "engine_rpm": 0,
                "odometer_km": random.uniform(10000, 200000),
                "status": "IDLE",
                "driver_id": f"DRV{random.randint(1, 200):04d}",
            })
        
        return vehicles
    
    def _generate_gps_reading(self, vehicle: dict) -> dict:
        """Generate a single GPS reading with realistic noise."""
        
        # Add small random movement (simulating travel)
        if vehicle["status"] == "MOVING":
            # Move towards destination
            lat_delta = (vehicle["destination_lat"] - vehicle["current_lat"]) * 0.001
            lng_delta = (vehicle["destination_lng"] - vehicle["current_lng"]) * 0.001
            
            vehicle["current_lat"] += lat_delta + random.gauss(0, 0.0001)
            vehicle["current_lng"] += lng_delta + random.gauss(0, 0.0001)
            vehicle["speed_kmh"] = random.gauss(60, 15)
            vehicle["engine_rpm"] = random.gauss(2500, 300)
            vehicle["fuel_level"] -= random.uniform(0.01, 0.05)
        
        # GPS reading with H3 index
        reading = {
            "vehicle_id": vehicle["vehicle_id"],
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "latitude": round(vehicle["current_lat"], 6),
            "longitude": round(vehicle["current_lng"], 6),
            "h3_index": h3.geo_to_h3(vehicle["current_lat"], vehicle["current_lng"], 9),
            "speed_kmh": max(0, round(vehicle["speed_kmh"], 1)),
            "heading_degrees": random.randint(0, 359),
            "altitude_m": random.randint(100, 500),
            "hdop": round(random.uniform(0.8, 2.5), 1),  # GPS accuracy
            "satellites": random.randint(6, 12),
            "driver_id": vehicle["driver_id"],
            "vehicle_type": vehicle["vehicle_type"],
        }
        
        return reading
    
    def _generate_telemetry_reading(self, vehicle: dict) -> dict:
        """Generate OBD-II style telemetry data."""
        
        return {
            "vehicle_id": vehicle["vehicle_id"],
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "engine_rpm": max(0, int(vehicle["engine_rpm"])),
            "fuel_level_pct": round(max(0, vehicle["fuel_level"]), 1),
            "coolant_temp_c": random.randint(80, 95) if vehicle["status"] == "MOVING" else 25,
            "odometer_km": round(vehicle["odometer_km"], 1),
            "battery_voltage": round(random.uniform(12.4, 14.4), 1),
            "engine_load_pct": random.randint(20, 80) if vehicle["status"] == "MOVING" else 0,
            "throttle_position_pct": random.randint(10, 90) if vehicle["status"] == "MOVING" else 0,
            "intake_air_temp_c": random.randint(25, 45),
        }
    
    def run(self, interval_seconds: int = 10):
        """Main loop - generate and publish GPS data."""
        
        print(f"Starting simulation for {self.num_vehicles} vehicles...")
        
        # Start some vehicles moving
        for v in random.sample(self.vehicles, int(self.num_vehicles * 0.7)):
            v["status"] = "MOVING"
        
        iteration = 0
        while True:
            for vehicle in self.vehicles:
                # GPS reading every iteration
                gps_reading = self._generate_gps_reading(vehicle)
                self.producer.send('vehicle_positions', gps_reading)
                
                # Telemetry every 3rd iteration (30 seconds if interval is 10)
                if iteration % 3 == 0:
                    telemetry = self._generate_telemetry_reading(vehicle)
                    self.producer.send('vehicle_telemetry', telemetry)
            
            self.producer.flush()
            iteration += 1
            time.sleep(interval_seconds)


if __name__ == "__main__":
    simulator = VehicleGPSSimulator(
        kafka_bootstrap="localhost:9092",
        num_vehicles=50
    )
    simulator.run(interval_seconds=10)
```

## 3.3 Shipment Event Simulator

```python
# src/simulators/shipment_event_simulator.py

"""
Generates realistic shipment events:
- Packages move through hub network
- Events: pickup, hub_arrival, hub_departure, out_for_delivery, delivered
- Realistic delays, exceptions, failed deliveries
"""

import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from enum import Enum

class ShipmentStatus(Enum):
    CREATED = "CREATED"
    PICKUP_SCHEDULED = "PICKUP_SCHEDULED"
    PICKED_UP = "PICKED_UP"
    IN_TRANSIT = "IN_TRANSIT"
    HUB_ARRIVED = "HUB_ARRIVED"
    HUB_DEPARTED = "HUB_DEPARTED"
    OUT_FOR_DELIVERY = "OUT_FOR_DELIVERY"
    DELIVERED = "DELIVERED"
    DELIVERY_FAILED = "DELIVERY_FAILED"
    RETURNED = "RETURNED"

class ShipmentEventSimulator:
    def __init__(self, kafka_bootstrap: str):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.shipments = {}
        self.shipment_counter = 0
        
        # Hub network
        self.hubs = {
            "HUB_DEL_01": {"city": "Delhi", "type": "MEGA", "lat": 28.6139, "lng": 77.2090},
            "HUB_DEL_02": {"city": "Delhi", "type": "SPOKE", "lat": 28.5500, "lng": 77.1500},
            "HUB_MUM_01": {"city": "Mumbai", "type": "MEGA", "lat": 19.0760, "lng": 72.8777},
            "HUB_MUM_02": {"city": "Mumbai", "type": "SPOKE", "lat": 19.1500, "lng": 72.9500},
            "HUB_BLR_01": {"city": "Bangalore", "type": "MEGA", "lat": 12.9716, "lng": 77.5946},
            "HUB_CHN_01": {"city": "Chennai", "type": "MEGA", "lat": 13.0827, "lng": 80.2707},
            "HUB_HYD_01": {"city": "Hyderabad", "type": "MEGA", "lat": 17.3850, "lng": 78.4867},
            "HUB_KOL_01": {"city": "Kolkata", "type": "MEGA", "lat": 22.5726, "lng": 88.3639},
        }
        
        # Routes between hubs (which hubs connect to which)
        self.routes = {
            "HUB_DEL_01": ["HUB_DEL_02", "HUB_MUM_01", "HUB_KOL_01", "HUB_HYD_01"],
            "HUB_MUM_01": ["HUB_MUM_02", "HUB_DEL_01", "HUB_BLR_01", "HUB_HYD_01"],
            "HUB_BLR_01": ["HUB_MUM_01", "HUB_CHN_01", "HUB_HYD_01"],
            "HUB_CHN_01": ["HUB_BLR_01", "HUB_HYD_01", "HUB_KOL_01"],
            "HUB_HYD_01": ["HUB_DEL_01", "HUB_MUM_01", "HUB_BLR_01", "HUB_CHN_01"],
            "HUB_KOL_01": ["HUB_DEL_01", "HUB_CHN_01"],
        }
    
    def _create_shipment(self) -> dict:
        """Create a new shipment."""
        self.shipment_counter += 1
        
        origin_hub = random.choice(list(self.hubs.keys()))
        dest_hub = random.choice([h for h in self.hubs.keys() if h != origin_hub])
        
        # Calculate route (simplified - just origin → mega hub → destination)
        route = self._calculate_route(origin_hub, dest_hub)
        
        shipment = {
            "shipment_id": f"SHP{self.shipment_counter:08d}",
            "awb_number": f"AWB{random.randint(100000000, 999999999)}",
            "order_id": f"ORD{random.randint(100000, 999999)}",
            "customer_id": f"CUST{random.randint(10000, 99999)}",
            "seller_id": f"SELL{random.randint(1000, 9999)}",
            "origin_hub": origin_hub,
            "destination_hub": dest_hub,
            "route": route,
            "current_hub_index": 0,
            "status": ShipmentStatus.CREATED.value,
            "created_at": datetime.utcnow().isoformat() + "Z",
            "promised_delivery": (datetime.utcnow() + timedelta(days=random.randint(2, 5))).isoformat() + "Z",
            "weight_kg": round(random.uniform(0.5, 30), 2),
            "dimensions_cm": f"{random.randint(10,50)}x{random.randint(10,50)}x{random.randint(10,50)}",
            "category": random.choice(["ELECTRONICS", "FASHION", "HOME", "BOOKS", "GROCERY"]),
        }
        
        return shipment
    
    def _calculate_route(self, origin: str, destination: str) -> list:
        """Calculate route through hub network."""
        # Simplified: origin → connecting mega hub → destination
        # In reality, this would use graph algorithms
        return [origin, destination]
    
    def _generate_event(self, shipment: dict, event_type: str, hub_id: str = None) -> dict:
        """Generate a shipment event."""
        
        hub = self.hubs.get(hub_id, {})
        
        event = {
            "event_id": f"EVT{random.randint(100000000, 999999999)}",
            "shipment_id": shipment["shipment_id"],
            "awb_number": shipment["awb_number"],
            "event_type": event_type,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "hub_id": hub_id,
            "hub_city": hub.get("city"),
            "latitude": hub.get("lat"),
            "longitude": hub.get("lng"),
            "worker_id": f"WRK{random.randint(1000, 9999)}",
            "device_id": f"SCAN{random.randint(100, 999)}",
            "metadata": {},
        }
        
        # Add event-specific metadata
        if event_type == "DELIVERY_FAILED":
            event["metadata"]["failure_reason"] = random.choice([
                "CUSTOMER_NOT_AVAILABLE",
                "WRONG_ADDRESS",
                "REFUSED_BY_CUSTOMER",
                "ACCESS_ISSUE",
            ])
        
        return event
    
    def _advance_shipment(self, shipment: dict):
        """Advance shipment to next state and generate events."""
        
        current_status = shipment["status"]
        route = shipment["route"]
        current_idx = shipment["current_hub_index"]
        
        # State machine
        if current_status == ShipmentStatus.CREATED.value:
            shipment["status"] = ShipmentStatus.PICKED_UP.value
            event = self._generate_event(shipment, "PICKED_UP", route[0])
            
        elif current_status == ShipmentStatus.PICKED_UP.value:
            shipment["status"] = ShipmentStatus.HUB_ARRIVED.value
            event = self._generate_event(shipment, "HUB_ARRIVED", route[current_idx])
            
        elif current_status == ShipmentStatus.HUB_ARRIVED.value:
            if current_idx < len(route) - 1:
                shipment["status"] = ShipmentStatus.HUB_DEPARTED.value
                event = self._generate_event(shipment, "HUB_DEPARTED", route[current_idx])
                shipment["current_hub_index"] += 1
            else:
                shipment["status"] = ShipmentStatus.OUT_FOR_DELIVERY.value
                event = self._generate_event(shipment, "OUT_FOR_DELIVERY", route[current_idx])
                
        elif current_status == ShipmentStatus.HUB_DEPARTED.value:
            shipment["status"] = ShipmentStatus.HUB_ARRIVED.value
            event = self._generate_event(shipment, "HUB_ARRIVED", route[current_idx])
            
        elif current_status == ShipmentStatus.OUT_FOR_DELIVERY.value:
            # 90% delivered, 10% failed
            if random.random() < 0.9:
                shipment["status"] = ShipmentStatus.DELIVERED.value
                event = self._generate_event(shipment, "DELIVERED", route[-1])
            else:
                shipment["status"] = ShipmentStatus.DELIVERY_FAILED.value
                event = self._generate_event(shipment, "DELIVERY_FAILED", route[-1])
        else:
            return None
        
        return event
    
    def run(self, shipments_per_minute: int = 10):
        """Main loop - create shipments and advance existing ones."""
        
        print(f"Starting shipment simulation ({shipments_per_minute} new shipments/min)...")
        
        while True:
            # Create new shipments
            for _ in range(shipments_per_minute):
                shipment = self._create_shipment()
                self.shipments[shipment["shipment_id"]] = shipment
                
                # Generate creation event
                event = self._generate_event(shipment, "CREATED", shipment["origin_hub"])
                self.producer.send('shipment_events', event)
            
            # Advance existing shipments
            completed = []
            for shipment_id, shipment in self.shipments.items():
                if shipment["status"] in [ShipmentStatus.DELIVERED.value, 
                                          ShipmentStatus.DELIVERY_FAILED.value,
                                          ShipmentStatus.RETURNED.value]:
                    completed.append(shipment_id)
                    continue
                
                # 30% chance to advance each iteration
                if random.random() < 0.3:
                    event = self._advance_shipment(shipment)
                    if event:
                        self.producer.send('shipment_events', event)
            
            # Remove completed shipments (keep memory bounded)
            for sid in completed:
                if random.random() < 0.1:  # Keep some for a while
                    del self.shipments[sid]
            
            self.producer.flush()
            time.sleep(60 / shipments_per_minute)


if __name__ == "__main__":
    simulator = ShipmentEventSimulator(kafka_bootstrap="localhost:9092")
    simulator.run(shipments_per_minute=10)
```

## 3.4 Delivery Agent Simulator

```python
# src/simulators/delivery_agent_simulator.py

"""
Generates realistic last-mile delivery data:
- Delivery agents with GPS tracking
- Order assignments
- Delivery attempts with success/failure
- Time at each stop
"""

import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
import h3

class DeliveryAgentSimulator:
    def __init__(self, kafka_bootstrap: str, num_agents: int = 100):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.num_agents = num_agents
        self.agents = self._initialize_agents()
        self.delivery_counter = 0
        
    def _initialize_agents(self):
        """Initialize delivery agents in different cities."""
        
        # Delivery zones (neighborhoods within cities)
        zones = [
            {"city": "Delhi", "area": "Connaught Place", "center_lat": 28.6315, "center_lng": 77.2167},
            {"city": "Delhi", "area": "Lajpat Nagar", "center_lat": 28.5700, "center_lng": 77.2400},
            {"city": "Delhi", "area": "Dwarka", "center_lat": 28.5921, "center_lng": 77.0460},
            {"city": "Mumbai", "area": "Andheri", "center_lat": 19.1136, "center_lng": 72.8697},
            {"city": "Mumbai", "area": "Bandra", "center_lat": 19.0596, "center_lng": 72.8295},
            {"city": "Bangalore", "area": "Koramangala", "center_lat": 12.9352, "center_lng": 77.6245},
            {"city": "Bangalore", "area": "Whitefield", "center_lat": 12.9698, "center_lng": 77.7500},
            {"city": "Chennai", "area": "T Nagar", "center_lat": 13.0418, "center_lng": 80.2341},
        ]
        
        agents = []
        for i in range(self.num_agents):
            zone = random.choice(zones)
            
            # Random position within zone (roughly 2km radius)
            lat = zone["center_lat"] + random.gauss(0, 0.01)
            lng = zone["center_lng"] + random.gauss(0, 0.01)
            
            agents.append({
                "agent_id": f"AGT{i+1:05d}",
                "name": f"Agent {i+1}",
                "phone": f"+91{random.randint(7000000000, 9999999999)}",
                "vehicle_type": random.choice(["BIKE", "SCOOTER", "BICYCLE"]),
                "zone": zone,
                "current_lat": lat,
                "current_lng": lng,
                "status": "IDLE",  # IDLE, EN_ROUTE, AT_STOP
                "current_orders": [],
                "completed_today": 0,
                "shift_start": datetime.utcnow(),
            })
        
        return agents
    
    def _assign_orders(self, agent: dict, num_orders: int = 5):
        """Assign orders to an agent."""
        
        zone = agent["zone"]
        orders = []
        
        for _ in range(num_orders):
            # Random delivery location within zone
            lat = zone["center_lat"] + random.gauss(0, 0.015)
            lng = zone["center_lng"] + random.gauss(0, 0.015)
            
            self.delivery_counter += 1
            
            orders.append({
                "order_id": f"ORD{self.delivery_counter:08d}",
                "shipment_id": f"SHP{random.randint(10000000, 99999999)}",
                "customer_name": f"Customer {self.delivery_counter}",
                "customer_phone": f"+91{random.randint(7000000000, 9999999999)}",
                "delivery_lat": lat,
                "delivery_lng": lng,
                "delivery_address": f"{random.randint(1, 500)}, {zone['area']}, {zone['city']}",
                "status": "ASSIGNED",
                "assigned_at": datetime.utcnow().isoformat() + "Z",
                "preferred_slot": random.choice(["MORNING", "AFTERNOON", "EVENING"]),
                "package_size": random.choice(["SMALL", "MEDIUM", "LARGE"]),
                "cod_amount": random.choice([0, 0, 0, random.randint(100, 5000)]),  # 25% COD
            })
        
        agent["current_orders"] = orders
        agent["status"] = "EN_ROUTE"
        
        return orders
    
    def _generate_position(self, agent: dict) -> dict:
        """Generate GPS position for agent."""
        
        # If agent has orders, move towards next delivery
        if agent["current_orders"] and agent["status"] == "EN_ROUTE":
            next_order = agent["current_orders"][0]
            
            # Move towards delivery location
            lat_diff = next_order["delivery_lat"] - agent["current_lat"]
            lng_diff = next_order["delivery_lng"] - agent["current_lng"]
            
            # Speed depends on vehicle type
            speed_factor = {"BIKE": 0.002, "SCOOTER": 0.0015, "BICYCLE": 0.001}[agent["vehicle_type"]]
            
            agent["current_lat"] += lat_diff * speed_factor + random.gauss(0, 0.0001)
            agent["current_lng"] += lng_diff * speed_factor + random.gauss(0, 0.0001)
            
            # Check if arrived at stop (within ~50m)
            if abs(lat_diff) < 0.0005 and abs(lng_diff) < 0.0005:
                agent["status"] = "AT_STOP"
        
        speed = 0
        if agent["status"] == "EN_ROUTE":
            speed = random.gauss(25, 5) if agent["vehicle_type"] == "BIKE" else random.gauss(15, 3)
        
        return {
            "agent_id": agent["agent_id"],
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "latitude": round(agent["current_lat"], 6),
            "longitude": round(agent["current_lng"], 6),
            "h3_index": h3.geo_to_h3(agent["current_lat"], agent["current_lng"], 9),
            "speed_kmh": max(0, round(speed, 1)),
            "accuracy_m": random.randint(5, 20),
            "battery_pct": random.randint(20, 100),
            "status": agent["status"],
            "vehicle_type": agent["vehicle_type"],
        }
    
    def _attempt_delivery(self, agent: dict) -> dict:
        """Attempt delivery of current order."""
        
        if not agent["current_orders"]:
            return None
        
        order = agent["current_orders"][0]
        
        # Determine outcome (85% success, 15% failure)
        success = random.random() < 0.85
        
        failure_reasons = [
            "CUSTOMER_NOT_AVAILABLE",
            "WRONG_ADDRESS",
            "CUSTOMER_REFUSED",
            "ACCESS_RESTRICTED",
            "PAYMENT_ISSUE",
        ]
        
        event = {
            "event_id": f"DEL{random.randint(100000000, 999999999)}",
            "agent_id": agent["agent_id"],
            "order_id": order["order_id"],
            "shipment_id": order["shipment_id"],
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "latitude": agent["current_lat"],
            "longitude": agent["current_lng"],
            "h3_index": h3.geo_to_h3(agent["current_lat"], agent["current_lng"], 9),
            "result": "DELIVERED" if success else "FAILED",
            "failure_reason": None if success else random.choice(failure_reasons),
            "time_at_stop_seconds": random.randint(60, 300),
            "pod_type": random.choice(["OTP", "SIGNATURE", "PHOTO"]) if success else None,
            "cod_collected": order["cod_amount"] if success and order["cod_amount"] > 0 else 0,
            "customer_rating": random.randint(3, 5) if success and random.random() < 0.3 else None,
        }
        
        # Remove completed order
        agent["current_orders"].pop(0)
        agent["completed_today"] += 1
        
        # Move to next order or go idle
        if agent["current_orders"]:
            agent["status"] = "EN_ROUTE"
        else:
            agent["status"] = "IDLE"
        
        return event
    
    def run(self, position_interval_seconds: int = 30):
        """Main loop."""
        
        print(f"Starting delivery simulation for {self.num_agents} agents...")
        
        iteration = 0
        while True:
            for agent in self.agents:
                # Assign new orders to idle agents
                if agent["status"] == "IDLE" and random.random() < 0.1:
                    orders = self._assign_orders(agent, num_orders=random.randint(5, 15))
                    for order in orders:
                        self.producer.send('delivery_events', {
                            "event_type": "ORDER_ASSIGNED",
                            "agent_id": agent["agent_id"],
                            "order_id": order["order_id"],
                            "timestamp": datetime.utcnow().isoformat() + "Z",
                        })
                
                # Generate position
                position = self._generate_position(agent)
                self.producer.send('agent_positions', position)
                
                # Attempt delivery if at stop
                if agent["status"] == "AT_STOP":
                    # Stay at stop for a bit
                    if random.random() < 0.3:
                        event = self._attempt_delivery(agent)
                        if event:
                            self.producer.send('delivery_events', event)
            
            self.producer.flush()
            iteration += 1
            time.sleep(position_interval_seconds)


if __name__ == "__main__":
    simulator = DeliveryAgentSimulator(
        kafka_bootstrap="localhost:9092",
        num_agents=100
    )
    simulator.run(position_interval_seconds=30)
```

## 3.5 Reference Data

```sql
-- Reference data loaded into PostgreSQL

-- Hubs
CREATE TABLE ref_hubs (
    hub_id VARCHAR(20) PRIMARY KEY,
    hub_name VARCHAR(100),
    hub_type VARCHAR(20),  -- MEGA, SPOKE, DELIVERY_CENTER
    city VARCHAR(50),
    state VARCHAR(50),
    pincode VARCHAR(10),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    capacity_packages_per_day INT,
    operating_hours VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE
);

-- Geofences
CREATE TABLE ref_geofences (
    geofence_id VARCHAR(20) PRIMARY KEY,
    geofence_name VARCHAR(100),
    geofence_type VARCHAR(20),  -- HUB, CITY_BOUNDARY, RESTRICTED, TOLL
    geometry GEOMETRY(POLYGON, 4326),  -- PostGIS
    speed_limit_kmh INT,
    is_active BOOLEAN DEFAULT TRUE
);

-- Vehicles
CREATE TABLE ref_vehicles (
    vehicle_id VARCHAR(20) PRIMARY KEY,
    registration_number VARCHAR(20),
    vehicle_type VARCHAR(20),
    make VARCHAR(50),
    model VARCHAR(50),
    year INT,
    fuel_type VARCHAR(20),
    capacity_kg DECIMAL(10, 2),
    gps_device_id VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE
);

-- Drivers
CREATE TABLE ref_drivers (
    driver_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100),
    phone VARCHAR(20),
    license_number VARCHAR(50),
    license_expiry DATE,
    home_hub_id VARCHAR(20) REFERENCES ref_hubs(hub_id),
    hire_date DATE,
    is_active BOOLEAN DEFAULT TRUE
);

-- Delivery Agents
CREATE TABLE ref_delivery_agents (
    agent_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100),
    phone VARCHAR(20),
    vehicle_type VARCHAR(20),
    zone_hub_id VARCHAR(20) REFERENCES ref_hubs(hub_id),
    hire_date DATE,
    is_active BOOLEAN DEFAULT TRUE
);

-- SLA Definitions
CREATE TABLE ref_sla_definitions (
    origin_city VARCHAR(50),
    destination_city VARCHAR(50),
    service_type VARCHAR(20),  -- EXPRESS, STANDARD, ECONOMY
    promised_days INT,
    PRIMARY KEY (origin_city, destination_city, service_type)
);
```

---

This completes Part 1. Let me continue with the Data Model in Part 2.
