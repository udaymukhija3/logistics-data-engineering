# src/config.py
"""
Configuration for Unified Logistics Data Platform.
"""

from pathlib import Path
import os

# ============================================
# PATHS
# ============================================

ROOT_DIR = Path(__file__).parent.parent
DATA_DIR = ROOT_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
CHECKPOINT_DIR = DATA_DIR / "checkpoints"

for d in [BRONZE_DIR, SILVER_DIR, GOLD_DIR, CHECKPOINT_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ============================================
# KAFKA CONFIGURATION
# ============================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

KAFKA_TOPICS = {
    # Fleet Telematics
    "vehicle_positions": "vehicle_positions",
    "vehicle_telemetry": "vehicle_telemetry",
    
    # Shipment Tracking
    "shipment_events": "shipment_events",
    
    # Last-Mile Delivery
    "agent_positions": "agent_positions",
    "delivery_events": "delivery_events",
    
    # Alerts (all modules)
    "alerts": "alerts",
}

# ============================================
# SPARK CONFIGURATION
# ============================================

SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_APP_NAME = "UnifiedLogisticsPlatform"

SPARK_CONFIG = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.shuffle.partitions": "8",  # Reduce for local dev
}

# ============================================
# GEOGRAPHIC BOUNDS (INDIA)
# ============================================

INDIA_BOUNDS = {
    "lat_min": 8.0,
    "lat_max": 37.0,
    "lng_min": 68.0,
    "lng_max": 97.5,
}

# ============================================
# H3 CONFIGURATION
# ============================================

H3_RESOLUTION = 9  # ~0.1 km² hexagons
H3_RESOLUTION_COARSE = 7  # ~5 km² for aggregations

# ============================================
# FLEET TELEMATICS CONFIG
# ============================================

FLEET_CONFIG = {
    # GPS
    "gps_interval_seconds": 10,
    "gps_valid_speed_max_kmh": 200,
    
    # Driving events
    "speeding_threshold_kmh": 80,
    "speeding_highway_threshold_kmh": 100,
    "harsh_brake_threshold_g": 0.4,
    "harsh_accel_threshold_g": 0.35,
    "idle_threshold_minutes": 5,
    
    # Trip detection
    "trip_gap_minutes": 30,
    "min_trip_duration_minutes": 5,
    "min_trip_positions": 5,
}

# ============================================
# SHIPMENT TRACKING CONFIG
# ============================================

SHIPMENT_CONFIG = {
    # SLA
    "stuck_threshold_hours": 24,
    "sla_at_risk_hours_before": 12,
    
    # Hub dwell
    "bottleneck_dwell_hours": 12,
    
    # Events
    "valid_event_types": [
        "CREATED", "PICKUP_SCHEDULED", "PICKED_UP",
        "HUB_ARRIVED", "HUB_INSCAN", "HUB_SORTED", "HUB_OUTSCAN", "HUB_DEPARTED",
        "IN_TRANSIT", "OUT_FOR_DELIVERY",
        "DELIVERY_ATTEMPTED", "DELIVERED", "DELIVERY_FAILED",
        "RETURNED_TO_ORIGIN", "LOST", "DAMAGED"
    ],
}

# ============================================
# LAST-MILE CONFIG
# ============================================

DELIVERY_CONFIG = {
    # Agent tracking
    "agent_gps_interval_seconds": 30,
    
    # Stop detection
    "stop_speed_threshold_kmh": 2,
    "stop_min_duration_seconds": 60,
    
    # Delivery
    "delivery_proximity_meters": 50,
    
    # Failure reasons
    "failure_reasons": [
        "CUSTOMER_NOT_AVAILABLE",
        "WRONG_ADDRESS",
        "ACCESS_RESTRICTED",
        "CUSTOMER_REFUSED",
        "PAYMENT_ISSUE",
        "DAMAGED_PACKAGE",
        "OTHER",
    ],
}

# ============================================
# ALERT CONFIG
# ============================================

ALERT_CONFIG = {
    "severity_levels": ["INFO", "WARNING", "HIGH", "CRITICAL"],
    
    "modules": ["FLEET", "SHIPMENT", "LAST_MILE"],
    
    # Slack webhook (set in .env)
    "slack_webhook_url": os.getenv("SLACK_WEBHOOK_URL"),
    
    "alert_channels": {
        "CRITICAL": ["slack", "sms"],
        "HIGH": ["slack"],
        "WARNING": ["slack"],
        "INFO": [],
    },
}

# ============================================
# SIMULATION CONFIG
# ============================================

SIMULATION_CONFIG = {
    "num_vehicles": 50,
    "num_delivery_agents": 100,
    "shipments_per_minute": 10,
    
    # Indian cities for simulation
    "hubs": [
        {"name": "Delhi Hub", "id": "HUB_DEL_01", "lat": 28.6139, "lng": 77.2090, "type": "MEGA"},
        {"name": "Mumbai Hub", "id": "HUB_MUM_01", "lat": 19.0760, "lng": 72.8777, "type": "MEGA"},
        {"name": "Bangalore Hub", "id": "HUB_BLR_01", "lat": 12.9716, "lng": 77.5946, "type": "MEGA"},
        {"name": "Chennai Hub", "id": "HUB_CHN_01", "lat": 13.0827, "lng": 80.2707, "type": "MEGA"},
        {"name": "Hyderabad Hub", "id": "HUB_HYD_01", "lat": 17.3850, "lng": 78.4867, "type": "MEGA"},
        {"name": "Kolkata Hub", "id": "HUB_KOL_01", "lat": 22.5726, "lng": 88.3639, "type": "MEGA"},
        {"name": "Pune Hub", "id": "HUB_PUN_01", "lat": 18.5204, "lng": 73.8567, "type": "REGIONAL"},
        {"name": "Ahmedabad Hub", "id": "HUB_AMD_01", "lat": 23.0225, "lng": 72.5714, "type": "REGIONAL"},
    ],
}

# ============================================
# DATABASE CONFIG
# ============================================

WAREHOUSE_CONFIG = {
    "duckdb_path": DATA_DIR / "warehouse" / "logistics.duckdb",
    "postgres_host": os.getenv("POSTGRES_HOST", "localhost"),
    "postgres_port": os.getenv("POSTGRES_PORT", "5432"),
    "postgres_db": os.getenv("POSTGRES_DB", "logistics"),
    "postgres_user": os.getenv("POSTGRES_USER", "postgres"),
    "postgres_password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}

# ============================================
# AIRFLOW CONFIG
# ============================================

AIRFLOW_CONFIG = {
    "dag_id_prefix": "logistics_",
    "default_retries": 3,
    "retry_delay_minutes": 5,
    "schedule_batch": "0 2 * * *",  # 2 AM daily
    "schedule_quality": "0 * * * *",  # Every hour
}
