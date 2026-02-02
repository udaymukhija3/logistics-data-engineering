# UNIFIED LOGISTICS DATA PLATFORM
## Part 2: Comprehensive Data Model

---

# SECTION 4: DATA MODEL (DIMENSIONAL)

## 4.1 Design Philosophy

This data model follows the **Kimball dimensional modeling** approach:
- **Fact tables** store measurements/events (what happened)
- **Dimension tables** store descriptive attributes (context)
- **Star schema** for fast analytical queries
- **Conformed dimensions** shared across fact tables

## 4.2 Entity Relationship Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      DIMENSIONAL MODEL OVERVIEW                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│                            DIMENSIONS                                        │
│                                                                              │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐               │
│  │dim_time   │  │dim_geo    │  │dim_hubs   │  │dim_vehicles│               │
│  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘               │
│        │              │              │              │                       │
│        │              │              │              │                       │
│  ┌─────┴─────┐  ┌─────┴─────┐  ┌─────┴─────┐  ┌─────┴─────┐               │
│  │dim_drivers│  │dim_agents │  │dim_shipmt │  │dim_custmr │               │
│  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘               │
│        │              │              │              │                       │
│        └──────────────┴──────────────┴──────────────┘                       │
│                              │                                               │
│                              ▼                                               │
│                          FACTS                                               │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    FLEET TELEMATICS FACTS                            │   │
│  │  • fact_vehicle_positions (GPS every 10 sec)                        │   │
│  │  • fact_vehicle_telemetry (OBD data every 30 sec)                   │   │
│  │  • fact_driving_events (speeding, harsh brake, idle)                │   │
│  │  • fact_geofence_events (entry, exit)                               │   │
│  │  • fact_trips (reconstructed journeys)                              │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    SHIPMENT TRACKING FACTS                           │   │
│  │  • fact_shipment_events (all scan events, append-only)              │   │
│  │  • fact_shipment_status (current state, updated)                    │   │
│  │  • fact_shipment_journeys (reconstructed path)                      │   │
│  │  • fact_hub_throughput (daily hub metrics)                          │   │
│  │  • fact_sla_tracking (SLA compliance)                               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    LAST-MILE DELIVERY FACTS                          │   │
│  │  • fact_agent_positions (GPS every 30 sec)                          │   │
│  │  • fact_delivery_attempts (each attempt with outcome)               │   │
│  │  • fact_agent_shifts (daily shift summary)                          │   │
│  │  • fact_area_performance (zone-level metrics)                       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    CROSS-MODULE FACTS                                │   │
│  │  • fact_alerts (all alerts across modules)                          │   │
│  │  • fact_end_to_end_journey (shipment from origin to delivery)       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4.3 DIMENSION TABLES

### dim_time (Date/Time Dimension)

```sql
-- Standard date dimension with logistics-specific attributes

CREATE TABLE dim_time (
    time_key INT PRIMARY KEY,  -- YYYYMMDDHH format
    
    -- Date components
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week_of_year INT NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,  -- 1=Monday, 7=Sunday
    day_name VARCHAR(20) NOT NULL,
    
    -- Hour components
    hour INT NOT NULL,  -- 0-23
    hour_12 INT NOT NULL,  -- 1-12
    am_pm VARCHAR(2) NOT NULL,
    
    -- Time bands
    time_band VARCHAR(20) NOT NULL,  -- EARLY_MORNING, MORNING, AFTERNOON, EVENING, NIGHT
    
    -- Logistics-specific flags
    is_weekend BOOLEAN NOT NULL,
    is_public_holiday BOOLEAN NOT NULL,
    is_peak_hour BOOLEAN NOT NULL,  -- 8-10 AM, 5-8 PM
    is_business_hour BOOLEAN NOT NULL,  -- 9 AM - 6 PM weekdays
    
    -- Seasonality
    season VARCHAR(20) NOT NULL,  -- For demand patterns
    is_festival_season BOOLEAN NOT NULL,  -- Diwali, etc.
    is_sale_season BOOLEAN NOT NULL  -- E-commerce sales
);
```

### dim_geography (H3 Hexagon Dimension)

```sql
-- H3 hexagon-based geography dimension for efficient geospatial queries

CREATE TABLE dim_geography (
    h3_index_res9 VARCHAR(20) PRIMARY KEY,  -- Resolution 9 (~0.1 km² area)
    h3_index_res7 VARCHAR(20) NOT NULL,      -- Resolution 7 (~5 km² area)
    h3_index_res5 VARCHAR(20) NOT NULL,      -- Resolution 5 (~253 km² area)
    
    -- Center coordinates
    center_latitude DECIMAL(10, 6) NOT NULL,
    center_longitude DECIMAL(10, 6) NOT NULL,
    
    -- Administrative boundaries
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(50) DEFAULT 'India',
    pincode VARCHAR(10),
    
    -- Zone attributes
    zone_type VARCHAR(20),  -- URBAN, SUBURBAN, RURAL
    serviceable BOOLEAN DEFAULT TRUE,
    delivery_zone_id VARCHAR(20),
    
    -- Derived metrics (updated periodically)
    avg_delivery_success_rate DECIMAL(5, 2),
    avg_delivery_time_minutes DECIMAL(10, 2),
    order_density_category VARCHAR(20)  -- HIGH, MEDIUM, LOW
);
```

### dim_hubs

```sql
-- Hub/warehouse dimension

CREATE TABLE dim_hubs (
    hub_key INT PRIMARY KEY,  -- Surrogate key
    hub_id VARCHAR(20) NOT NULL,  -- Natural key
    
    -- Descriptive attributes
    hub_name VARCHAR(100) NOT NULL,
    hub_type VARCHAR(20) NOT NULL,  -- MEGA_HUB, REGIONAL_HUB, SPOKE, DELIVERY_CENTER
    
    -- Location
    address TEXT,
    city VARCHAR(50) NOT NULL,
    state VARCHAR(50) NOT NULL,
    pincode VARCHAR(10),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    h3_index_res9 VARCHAR(20),
    
    -- Operational attributes
    capacity_packages_per_day INT,
    operating_hours VARCHAR(50),  -- "06:00-22:00"
    num_docks INT,
    num_sorting_lines INT,
    
    -- Hierarchy
    parent_hub_id VARCHAR(20),  -- For spoke → regional → mega
    region VARCHAR(50),
    zone VARCHAR(50),
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    opened_date DATE,
    closed_date DATE,
    
    -- SCD Type 2 fields
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);
```

### dim_vehicles

```sql
-- Vehicle/fleet dimension

CREATE TABLE dim_vehicles (
    vehicle_key INT PRIMARY KEY,  -- Surrogate key
    vehicle_id VARCHAR(20) NOT NULL,  -- Natural key
    
    -- Registration
    registration_number VARCHAR(20) NOT NULL,
    registration_state VARCHAR(50),
    
    -- Vehicle attributes
    vehicle_type VARCHAR(20) NOT NULL,  -- TRUCK_20FT, TRUCK_32FT, VAN, PICKUP
    vehicle_category VARCHAR(20) NOT NULL,  -- LONG_HAUL, SHORT_HAUL, LOCAL
    make VARCHAR(50),
    model VARCHAR(50),
    year INT,
    
    -- Capacity
    capacity_kg DECIMAL(10, 2),
    capacity_cubic_m DECIMAL(10, 2),
    
    -- Fuel
    fuel_type VARCHAR(20),  -- DIESEL, PETROL, CNG, ELECTRIC
    fuel_tank_capacity_liters DECIMAL(10, 2),
    avg_mileage_kmpl DECIMAL(5, 2),
    
    -- Tracking device
    gps_device_id VARCHAR(50),
    gps_device_type VARCHAR(50),
    obd_device_id VARCHAR(50),
    
    -- Ownership
    ownership_type VARCHAR(20),  -- OWNED, LEASED, ATTACHED
    vendor_id VARCHAR(20),
    
    -- Home base
    home_hub_id VARCHAR(20),
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    
    -- SCD Type 2
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);
```

### dim_drivers

```sql
-- Driver dimension

CREATE TABLE dim_drivers (
    driver_key INT PRIMARY KEY,
    driver_id VARCHAR(20) NOT NULL,
    
    -- Personal info
    name VARCHAR(100) NOT NULL,
    phone VARCHAR(20),
    email VARCHAR(100),
    
    -- License
    license_number VARCHAR(50),
    license_type VARCHAR(20),  -- LMV, HMV
    license_expiry_date DATE,
    
    -- Employment
    employment_type VARCHAR(20),  -- FULL_TIME, CONTRACT, FREELANCE
    hire_date DATE,
    home_hub_id VARCHAR(20),
    
    -- Qualifications
    hazmat_certified BOOLEAN DEFAULT FALSE,
    long_haul_certified BOOLEAN DEFAULT FALSE,
    
    -- Derived metrics (updated daily)
    total_trips INT DEFAULT 0,
    total_distance_km DECIMAL(12, 2) DEFAULT 0,
    avg_safety_score DECIMAL(5, 2),
    avg_rating DECIMAL(3, 2),
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    
    -- SCD Type 2
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);
```

### dim_delivery_agents

```sql
-- Last-mile delivery agent dimension

CREATE TABLE dim_delivery_agents (
    agent_key INT PRIMARY KEY,
    agent_id VARCHAR(20) NOT NULL,
    
    -- Personal info
    name VARCHAR(100) NOT NULL,
    phone VARCHAR(20),
    
    -- Vehicle
    vehicle_type VARCHAR(20) NOT NULL,  -- BIKE, SCOOTER, BICYCLE, FOOT
    vehicle_registration VARCHAR(20),
    
    -- Assignment
    home_hub_id VARCHAR(20),
    primary_zone_h3 VARCHAR(20),
    
    -- Employment
    employment_type VARCHAR(20),  -- FULL_TIME, GIG
    hire_date DATE,
    
    -- Derived metrics (updated daily)
    total_deliveries INT DEFAULT 0,
    avg_deliveries_per_day DECIMAL(5, 2),
    first_attempt_success_rate DECIMAL(5, 2),
    avg_customer_rating DECIMAL(3, 2),
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    
    -- SCD Type 2
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);
```

### dim_shipments

```sql
-- Shipment dimension (for completed shipments)

CREATE TABLE dim_shipments (
    shipment_key BIGINT PRIMARY KEY,
    shipment_id VARCHAR(30) NOT NULL,
    awb_number VARCHAR(30) NOT NULL,
    
    -- Order linkage
    order_id VARCHAR(30),
    
    -- Parties
    seller_id VARCHAR(20),
    seller_name VARCHAR(100),
    customer_id VARCHAR(20),
    customer_name VARCHAR(100),
    
    -- Origin
    origin_hub_id VARCHAR(20),
    origin_city VARCHAR(50),
    origin_state VARCHAR(50),
    origin_pincode VARCHAR(10),
    
    -- Destination
    destination_hub_id VARCHAR(20),
    destination_city VARCHAR(50),
    destination_state VARCHAR(50),
    destination_pincode VARCHAR(10),
    destination_latitude DECIMAL(10, 6),
    destination_longitude DECIMAL(10, 6),
    destination_h3_index VARCHAR(20),
    
    -- Package details
    weight_kg DECIMAL(10, 3),
    length_cm DECIMAL(10, 2),
    width_cm DECIMAL(10, 2),
    height_cm DECIMAL(10, 2),
    volumetric_weight_kg DECIMAL(10, 3),
    
    -- Category
    product_category VARCHAR(50),
    is_fragile BOOLEAN DEFAULT FALSE,
    is_hazardous BOOLEAN DEFAULT FALSE,
    
    -- Value
    declared_value DECIMAL(12, 2),
    cod_amount DECIMAL(12, 2) DEFAULT 0,
    is_cod BOOLEAN DEFAULT FALSE,
    
    -- Service
    service_type VARCHAR(20),  -- EXPRESS, STANDARD, ECONOMY
    promised_delivery_date DATE,
    
    -- Timestamps
    created_at TIMESTAMP,
    picked_up_at TIMESTAMP,
    delivered_at TIMESTAMP,
    
    -- Final status
    final_status VARCHAR(20),  -- DELIVERED, RETURNED, LOST
    
    -- Metrics
    total_transit_hours DECIMAL(10, 2),
    total_hubs_visited INT,
    delivery_attempts INT
);
```

### dim_customers

```sql
-- Customer dimension

CREATE TABLE dim_customers (
    customer_key INT PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    
    -- Info (limited PII)
    customer_name VARCHAR(100),
    customer_type VARCHAR(20),  -- INDIVIDUAL, BUSINESS
    
    -- Primary location
    city VARCHAR(50),
    state VARCHAR(50),
    pincode VARCHAR(10),
    h3_index_res9 VARCHAR(20),
    
    -- Derived metrics
    total_orders INT DEFAULT 0,
    first_order_date DATE,
    last_order_date DATE,
    avg_order_value DECIMAL(12, 2),
    delivery_success_rate DECIMAL(5, 2),
    preferred_delivery_slot VARCHAR(20),
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE
);
```

### dim_sellers

```sql
-- Seller/shipper dimension

CREATE TABLE dim_sellers (
    seller_key INT PRIMARY KEY,
    seller_id VARCHAR(20) NOT NULL,
    
    -- Info
    seller_name VARCHAR(100),
    seller_type VARCHAR(20),  -- SMB, ENTERPRISE, MARKETPLACE
    
    -- Primary location
    city VARCHAR(50),
    state VARCHAR(50),
    pincode VARCHAR(10),
    primary_hub_id VARCHAR(20),
    
    -- Derived metrics
    total_shipments INT DEFAULT 0,
    avg_daily_shipments DECIMAL(10, 2),
    avg_weight_kg DECIMAL(10, 2),
    on_time_pickup_rate DECIMAL(5, 2),
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE
);
```

---

## 4.4 FACT TABLES: FLEET TELEMATICS

### fact_vehicle_positions

```sql
-- High-frequency GPS data (every 10 seconds per vehicle)
-- This is a MASSIVE table - partition by date

CREATE TABLE fact_vehicle_positions (
    position_id BIGINT PRIMARY KEY,  -- Generated
    
    -- Keys
    vehicle_key INT REFERENCES dim_vehicles(vehicle_key),
    driver_key INT REFERENCES dim_drivers(driver_key),
    time_key INT REFERENCES dim_time(time_key),
    geo_key VARCHAR(20) REFERENCES dim_geography(h3_index_res9),
    
    -- Timestamp (for partitioning)
    event_timestamp TIMESTAMP NOT NULL,
    
    -- Position
    latitude DECIMAL(10, 6) NOT NULL,
    longitude DECIMAL(10, 6) NOT NULL,
    h3_index_res9 VARCHAR(20) NOT NULL,
    
    -- Movement
    speed_kmh DECIMAL(6, 2),
    heading_degrees INT,
    altitude_m INT,
    
    -- GPS quality
    hdop DECIMAL(4, 2),
    satellites INT,
    gps_fix_type VARCHAR(10),  -- 2D, 3D
    
    -- Status
    ignition_on BOOLEAN,
    
    -- Validation
    is_valid_position BOOLEAN DEFAULT TRUE,
    validation_flags VARCHAR(50)  -- Comma-separated flags if invalid
    
) PARTITION BY RANGE (event_timestamp);

-- Create monthly partitions
CREATE TABLE fact_vehicle_positions_202501 PARTITION OF fact_vehicle_positions
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
```

### fact_vehicle_telemetry

```sql
-- OBD-II telemetry data (every 30 seconds)

CREATE TABLE fact_vehicle_telemetry (
    telemetry_id BIGINT PRIMARY KEY,
    
    -- Keys
    vehicle_key INT REFERENCES dim_vehicles(vehicle_key),
    time_key INT REFERENCES dim_time(time_key),
    
    -- Timestamp
    event_timestamp TIMESTAMP NOT NULL,
    
    -- Engine
    engine_rpm INT,
    engine_load_pct DECIMAL(5, 2),
    throttle_position_pct DECIMAL(5, 2),
    
    -- Temperature
    coolant_temp_c INT,
    intake_air_temp_c INT,
    ambient_air_temp_c INT,
    
    -- Fuel
    fuel_level_pct DECIMAL(5, 2),
    fuel_rate_lph DECIMAL(6, 2),  -- Liters per hour
    
    -- Distance
    odometer_km DECIMAL(12, 2),
    trip_distance_km DECIMAL(10, 2),
    
    -- Battery
    battery_voltage DECIMAL(4, 2),
    
    -- Calculated
    instant_fuel_economy_kmpl DECIMAL(6, 2)
    
) PARTITION BY RANGE (event_timestamp);
```

### fact_driving_events

```sql
-- Driving behavior events (speeding, harsh braking, etc.)

CREATE TABLE fact_driving_events (
    event_id BIGINT PRIMARY KEY,
    
    -- Keys
    vehicle_key INT REFERENCES dim_vehicles(vehicle_key),
    driver_key INT REFERENCES dim_drivers(driver_key),
    time_key INT REFERENCES dim_time(time_key),
    geo_key VARCHAR(20) REFERENCES dim_geography(h3_index_res9),
    
    -- Timestamp
    event_timestamp TIMESTAMP NOT NULL,
    
    -- Location
    latitude DECIMAL(10, 6) NOT NULL,
    longitude DECIMAL(10, 6) NOT NULL,
    h3_index_res9 VARCHAR(20) NOT NULL,
    
    -- Event details
    event_type VARCHAR(30) NOT NULL,  -- SPEEDING, HARSH_BRAKE, HARSH_ACCEL, EXCESSIVE_IDLE, FATIGUE_DRIVING
    severity VARCHAR(10) NOT NULL,  -- LOW, MEDIUM, HIGH, CRITICAL
    
    -- Measurements
    speed_kmh DECIMAL(6, 2),
    speed_limit_kmh DECIMAL(6, 2),  -- If available
    acceleration_g DECIMAL(5, 3),  -- For harsh brake/accel
    duration_seconds INT,  -- For idle, fatigue
    
    -- Context
    road_type VARCHAR(20),  -- HIGHWAY, URBAN, RURAL
    weather_condition VARCHAR(20),
    
    -- Trip context
    trip_id BIGINT
);
```

### fact_geofence_events

```sql
-- Geofence entry/exit events

CREATE TABLE fact_geofence_events (
    event_id BIGINT PRIMARY KEY,
    
    -- Keys
    vehicle_key INT REFERENCES dim_vehicles(vehicle_key),
    driver_key INT REFERENCES dim_drivers(driver_key),
    time_key INT REFERENCES dim_time(time_key),
    hub_key INT REFERENCES dim_hubs(hub_key),  -- If geofence is a hub
    
    -- Timestamp
    event_timestamp TIMESTAMP NOT NULL,
    
    -- Geofence
    geofence_id VARCHAR(20) NOT NULL,
    geofence_name VARCHAR(100),
    geofence_type VARCHAR(20),  -- HUB, CITY, RESTRICTED, TOLL, CUSTOMER
    
    -- Event
    event_type VARCHAR(10) NOT NULL,  -- ENTRY, EXIT
    
    -- Location at event
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    
    -- For EXIT events
    dwell_time_seconds INT,  -- Time spent inside
    
    -- Trip context
    trip_id BIGINT
);
```

### fact_trips

```sql
-- Reconstructed trips (batch processed)

CREATE TABLE fact_trips (
    trip_id BIGINT PRIMARY KEY,
    
    -- Keys
    vehicle_key INT REFERENCES dim_vehicles(vehicle_key),
    driver_key INT REFERENCES dim_drivers(driver_key),
    origin_hub_key INT REFERENCES dim_hubs(hub_key),
    destination_hub_key INT REFERENCES dim_hubs(hub_key),
    
    -- Time
    start_timestamp TIMESTAMP NOT NULL,
    end_timestamp TIMESTAMP NOT NULL,
    date_key INT,  -- Date of trip start
    
    -- Origin
    start_latitude DECIMAL(10, 6),
    start_longitude DECIMAL(10, 6),
    start_h3_index VARCHAR(20),
    start_geofence_id VARCHAR(20),
    
    -- Destination
    end_latitude DECIMAL(10, 6),
    end_longitude DECIMAL(10, 6),
    end_h3_index VARCHAR(20),
    end_geofence_id VARCHAR(20),
    
    -- Distance & Duration
    total_distance_km DECIMAL(10, 2),
    total_duration_minutes DECIMAL(10, 2),
    driving_duration_minutes DECIMAL(10, 2),
    idle_duration_minutes DECIMAL(10, 2),
    stop_duration_minutes DECIMAL(10, 2),
    
    -- Speed
    avg_speed_kmh DECIMAL(6, 2),
    max_speed_kmh DECIMAL(6, 2),
    
    -- Fuel
    fuel_consumed_liters DECIMAL(10, 2),
    fuel_economy_kmpl DECIMAL(6, 2),
    start_fuel_level_pct DECIMAL(5, 2),
    end_fuel_level_pct DECIMAL(5, 2),
    
    -- Driving behavior
    harsh_brake_count INT DEFAULT 0,
    harsh_accel_count INT DEFAULT 0,
    speeding_count INT DEFAULT 0,
    speeding_duration_minutes DECIMAL(10, 2) DEFAULT 0,
    
    -- Stops
    num_stops INT DEFAULT 0,
    
    -- Route adherence
    route_deviation_km DECIMAL(10, 2),
    route_adherence_pct DECIMAL(5, 2),
    
    -- Shipments carried
    num_shipments INT,
    total_weight_kg DECIMAL(10, 2),
    
    -- Status
    trip_status VARCHAR(20)  -- COMPLETED, INCOMPLETE, CANCELLED
);
```

---

## 4.5 FACT TABLES: SHIPMENT TRACKING

### fact_shipment_events

```sql
-- All shipment scan events (append-only event store)
-- This is the source of truth - immutable

CREATE TABLE fact_shipment_events (
    event_id BIGINT PRIMARY KEY,
    
    -- Shipment reference
    shipment_id VARCHAR(30) NOT NULL,
    awb_number VARCHAR(30) NOT NULL,
    
    -- Time
    event_timestamp TIMESTAMP NOT NULL,
    time_key INT REFERENCES dim_time(time_key),
    
    -- Event details
    event_type VARCHAR(30) NOT NULL,
    -- CREATED, PICKUP_SCHEDULED, PICKED_UP, 
    -- HUB_ARRIVED, HUB_INSCAN, HUB_SORTED, HUB_OUTSCAN, HUB_DEPARTED,
    -- IN_TRANSIT, OUT_FOR_DELIVERY,
    -- DELIVERY_ATTEMPTED, DELIVERED, DELIVERY_FAILED,
    -- RETURNED_TO_ORIGIN, LOST, DAMAGED
    
    event_sub_type VARCHAR(30),  -- For failures: CUSTOMER_NOT_AVAILABLE, WRONG_ADDRESS, etc.
    
    -- Location
    hub_key INT REFERENCES dim_hubs(hub_key),
    hub_id VARCHAR(20),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    h3_index VARCHAR(20),
    
    -- Actor
    worker_id VARCHAR(20),
    device_id VARCHAR(20),
    vehicle_id VARCHAR(20),
    
    -- Metadata
    metadata JSONB,  -- Flexible additional data
    
    -- Processing
    processed_at TIMESTAMP,
    batch_id VARCHAR(50)
    
) PARTITION BY RANGE (event_timestamp);
```

### fact_shipment_status

```sql
-- Current shipment status (materialized from events, updated)

CREATE TABLE fact_shipment_status (
    shipment_id VARCHAR(30) PRIMARY KEY,
    shipment_key BIGINT REFERENCES dim_shipments(shipment_key),
    
    -- Current state
    current_status VARCHAR(30) NOT NULL,
    current_hub_key INT REFERENCES dim_hubs(hub_key),
    current_hub_id VARCHAR(20),
    current_latitude DECIMAL(10, 6),
    current_longitude DECIMAL(10, 6),
    
    -- Last event
    last_event_id BIGINT,
    last_event_type VARCHAR(30),
    last_event_timestamp TIMESTAMP,
    
    -- Timing
    created_at TIMESTAMP,
    picked_up_at TIMESTAMP,
    last_hub_arrival_at TIMESTAMP,
    out_for_delivery_at TIMESTAMP,
    delivered_at TIMESTAMP,
    
    -- SLA
    promised_delivery_date DATE,
    promised_delivery_timestamp TIMESTAMP,
    is_sla_at_risk BOOLEAN DEFAULT FALSE,
    is_sla_breached BOOLEAN DEFAULT FALSE,
    sla_breach_hours DECIMAL(10, 2),
    
    -- Progress
    total_hubs_visited INT DEFAULT 0,
    expected_remaining_hubs INT,
    
    -- Health
    hours_since_last_event DECIMAL(10, 2),
    is_stuck BOOLEAN DEFAULT FALSE,  -- No movement in 24+ hours
    is_exception BOOLEAN DEFAULT FALSE,
    exception_reason VARCHAR(100),
    
    -- Delivery
    delivery_attempts INT DEFAULT 0,
    
    -- Updated timestamp
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for common queries
CREATE INDEX idx_shipment_status_current ON fact_shipment_status(current_status);
CREATE INDEX idx_shipment_status_stuck ON fact_shipment_status(is_stuck) WHERE is_stuck = TRUE;
CREATE INDEX idx_shipment_status_sla ON fact_shipment_status(is_sla_at_risk) WHERE is_sla_at_risk = TRUE;
```

### fact_shipment_journeys

```sql
-- Reconstructed shipment journeys (hop by hop)

CREATE TABLE fact_shipment_journeys (
    journey_id BIGINT PRIMARY KEY,
    shipment_id VARCHAR(30) NOT NULL,
    shipment_key BIGINT REFERENCES dim_shipments(shipment_key),
    
    -- Hop details
    hop_sequence INT NOT NULL,  -- 1, 2, 3, ...
    
    -- Hub
    hub_key INT REFERENCES dim_hubs(hub_key),
    hub_id VARCHAR(20),
    hub_name VARCHAR(100),
    hub_city VARCHAR(50),
    
    -- Timestamps
    arrival_timestamp TIMESTAMP,
    departure_timestamp TIMESTAMP,
    
    -- Metrics
    dwell_time_hours DECIMAL(10, 2),
    processing_time_hours DECIMAL(10, 2),  -- Inscan to outscan
    
    -- Flags
    is_bottleneck BOOLEAN DEFAULT FALSE,  -- Dwell > threshold
    is_origin BOOLEAN DEFAULT FALSE,
    is_destination BOOLEAN DEFAULT FALSE,
    
    -- Transport
    inbound_vehicle_id VARCHAR(20),
    outbound_vehicle_id VARCHAR(20),
    
    UNIQUE(shipment_id, hop_sequence)
);
```

### fact_hub_throughput

```sql
-- Daily hub metrics

CREATE TABLE fact_hub_throughput (
    throughput_id BIGINT PRIMARY KEY,
    
    -- Keys
    hub_key INT REFERENCES dim_hubs(hub_key),
    date_key INT REFERENCES dim_time(time_key),
    date DATE NOT NULL,
    
    -- Volume
    packages_received INT DEFAULT 0,
    packages_dispatched INT DEFAULT 0,
    packages_processed INT DEFAULT 0,  -- Scanned/sorted
    
    -- Inventory
    opening_inventory INT DEFAULT 0,
    closing_inventory INT DEFAULT 0,
    
    -- Timing
    avg_dwell_time_hours DECIMAL(10, 2),
    max_dwell_time_hours DECIMAL(10, 2),
    avg_processing_time_hours DECIMAL(10, 2),
    
    -- Exceptions
    stuck_packages_count INT DEFAULT 0,
    damaged_packages_count INT DEFAULT 0,
    lost_packages_count INT DEFAULT 0,
    
    -- Capacity
    capacity_utilization_pct DECIMAL(5, 2),
    
    UNIQUE(hub_key, date)
);
```

### fact_sla_tracking

```sql
-- SLA compliance tracking

CREATE TABLE fact_sla_tracking (
    sla_id BIGINT PRIMARY KEY,
    
    -- Keys
    shipment_key BIGINT REFERENCES dim_shipments(shipment_key),
    shipment_id VARCHAR(30) NOT NULL,
    
    -- Route
    origin_city VARCHAR(50),
    destination_city VARCHAR(50),
    service_type VARCHAR(20),
    
    -- Dates
    created_date DATE,
    promised_date DATE,
    actual_delivery_date DATE,
    
    -- SLA calculation
    promised_days INT,
    actual_days INT,
    variance_days INT,  -- Negative = early, positive = late
    
    -- Status
    sla_status VARCHAR(20),  -- MET, BREACHED, AT_RISK, PENDING
    
    -- Breach details
    breach_reason VARCHAR(100),  -- WEATHER, HUB_DELAY, LAST_MILE, etc.
    breach_hub_id VARCHAR(20),  -- Where the delay occurred
    
    -- Flags
    is_delivered BOOLEAN,
    is_on_time BOOLEAN
);
```

---

## 4.6 FACT TABLES: LAST-MILE DELIVERY

### fact_agent_positions

```sql
-- Delivery agent GPS positions

CREATE TABLE fact_agent_positions (
    position_id BIGINT PRIMARY KEY,
    
    -- Keys
    agent_key INT REFERENCES dim_delivery_agents(agent_key),
    time_key INT REFERENCES dim_time(time_key),
    geo_key VARCHAR(20) REFERENCES dim_geography(h3_index_res9),
    
    -- Timestamp
    event_timestamp TIMESTAMP NOT NULL,
    
    -- Position
    latitude DECIMAL(10, 6) NOT NULL,
    longitude DECIMAL(10, 6) NOT NULL,
    h3_index_res9 VARCHAR(20) NOT NULL,
    
    -- Movement
    speed_kmh DECIMAL(6, 2),
    
    -- Status
    activity_status VARCHAR(20),  -- TRAVELING, AT_STOP, IDLE
    
    -- Device
    battery_pct INT,
    accuracy_m INT
    
) PARTITION BY RANGE (event_timestamp);
```

### fact_delivery_attempts

```sql
-- Each delivery attempt

CREATE TABLE fact_delivery_attempts (
    attempt_id BIGINT PRIMARY KEY,
    
    -- Keys
    agent_key INT REFERENCES dim_delivery_agents(agent_key),
    shipment_key BIGINT REFERENCES dim_shipments(shipment_key),
    customer_key INT REFERENCES dim_customers(customer_key),
    time_key INT REFERENCES dim_time(time_key),
    geo_key VARCHAR(20) REFERENCES dim_geography(h3_index_res9),
    
    -- Identifiers
    shipment_id VARCHAR(30) NOT NULL,
    order_id VARCHAR(30),
    
    -- Attempt details
    attempt_number INT NOT NULL,  -- 1, 2, 3
    attempt_timestamp TIMESTAMP NOT NULL,
    
    -- Location
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    h3_index_res9 VARCHAR(20),
    
    -- Outcome
    result VARCHAR(20) NOT NULL,  -- DELIVERED, FAILED, PARTIAL
    failure_reason VARCHAR(50),  -- CUSTOMER_NOT_AVAILABLE, WRONG_ADDRESS, etc.
    
    -- Timing
    arrival_timestamp TIMESTAMP,
    departure_timestamp TIMESTAMP,
    time_at_stop_seconds INT,
    
    -- POD (Proof of Delivery)
    pod_type VARCHAR(20),  -- OTP, SIGNATURE, PHOTO
    pod_captured BOOLEAN DEFAULT FALSE,
    
    -- Payment
    cod_amount DECIMAL(12, 2) DEFAULT 0,
    cod_collected DECIMAL(12, 2) DEFAULT 0,
    payment_mode VARCHAR(20),  -- CASH, UPI, PREPAID
    
    -- Customer feedback
    customer_rating INT,  -- 1-5
    customer_feedback TEXT,
    
    -- Context
    slot_type VARCHAR(20),  -- MORNING, AFTERNOON, EVENING
    is_within_slot BOOLEAN,
    
    -- Sequence
    stop_sequence INT  -- Order of delivery in agent's route
);
```

### fact_agent_shifts

```sql
-- Daily shift summary per agent

CREATE TABLE fact_agent_shifts (
    shift_id BIGINT PRIMARY KEY,
    
    -- Keys
    agent_key INT REFERENCES dim_delivery_agents(agent_key),
    date_key INT REFERENCES dim_time(time_key),
    hub_key INT REFERENCES dim_hubs(hub_key),
    
    -- Date
    shift_date DATE NOT NULL,
    
    -- Time
    shift_start_timestamp TIMESTAMP,
    shift_end_timestamp TIMESTAMP,
    total_shift_hours DECIMAL(5, 2),
    active_hours DECIMAL(5, 2),  -- Actually working
    idle_hours DECIMAL(5, 2),
    break_hours DECIMAL(5, 2),
    
    -- Orders
    orders_assigned INT DEFAULT 0,
    orders_delivered INT DEFAULT 0,
    orders_failed INT DEFAULT 0,
    orders_rescheduled INT DEFAULT 0,
    first_attempt_success_rate DECIMAL(5, 2),
    
    -- Distance
    total_distance_km DECIMAL(10, 2),
    
    -- Earnings (if applicable)
    base_pay DECIMAL(10, 2),
    incentive_pay DECIMAL(10, 2),
    total_pay DECIMAL(10, 2),
    cod_collected DECIMAL(12, 2),
    
    -- Performance
    deliveries_per_hour DECIMAL(5, 2),
    avg_time_per_delivery_minutes DECIMAL(10, 2),
    avg_customer_rating DECIMAL(3, 2),
    
    UNIQUE(agent_key, shift_date)
);
```

### fact_area_performance

```sql
-- Zone/area level delivery metrics

CREATE TABLE fact_area_performance (
    area_perf_id BIGINT PRIMARY KEY,
    
    -- Keys
    geo_key VARCHAR(20) REFERENCES dim_geography(h3_index_res9),
    date_key INT REFERENCES dim_time(time_key),
    hub_key INT REFERENCES dim_hubs(hub_key),  -- Serving hub
    
    -- Date
    date DATE NOT NULL,
    
    -- Volume
    total_attempts INT DEFAULT 0,
    successful_deliveries INT DEFAULT 0,
    failed_deliveries INT DEFAULT 0,
    
    -- Rates
    success_rate DECIMAL(5, 2),
    first_attempt_success_rate DECIMAL(5, 2),
    
    -- Timing
    avg_delivery_time_minutes DECIMAL(10, 2),
    avg_time_at_stop_seconds INT,
    
    -- Failures breakdown
    failures_customer_not_available INT DEFAULT 0,
    failures_wrong_address INT DEFAULT 0,
    failures_access_issue INT DEFAULT 0,
    failures_refused INT DEFAULT 0,
    failures_other INT DEFAULT 0,
    
    -- Best time to deliver
    best_slot VARCHAR(20),  -- MORNING, AFTERNOON, EVENING
    best_slot_success_rate DECIMAL(5, 2),
    
    -- Customer satisfaction
    avg_customer_rating DECIMAL(3, 2),
    
    UNIQUE(geo_key, date)
);
```

---

## 4.7 CROSS-MODULE FACT TABLES

### fact_alerts

```sql
-- Unified alert table across all modules

CREATE TABLE fact_alerts (
    alert_id BIGINT PRIMARY KEY,
    
    -- Time
    alert_timestamp TIMESTAMP NOT NULL,
    time_key INT REFERENCES dim_time(time_key),
    
    -- Source module
    module VARCHAR(20) NOT NULL,  -- FLEET, SHIPMENT, LAST_MILE
    
    -- Alert type
    alert_type VARCHAR(50) NOT NULL,
    -- Fleet: SPEEDING, HARSH_BRAKE, GEOFENCE_BREACH, ROUTE_DEVIATION, EXCESSIVE_IDLE
    -- Shipment: SLA_AT_RISK, SLA_BREACHED, STUCK_PACKAGE, EXCEPTION
    -- Last-mile: DELIVERY_FAILED, AGENT_IDLE, LOW_PRODUCTIVITY
    
    alert_severity VARCHAR(10) NOT NULL,  -- INFO, WARNING, CRITICAL
    
    -- Entity
    entity_type VARCHAR(20) NOT NULL,  -- VEHICLE, SHIPMENT, AGENT
    entity_id VARCHAR(30) NOT NULL,
    
    -- Related dimensions
    vehicle_key INT REFERENCES dim_vehicles(vehicle_key),
    shipment_key BIGINT REFERENCES dim_shipments(shipment_key),
    agent_key INT REFERENCES dim_delivery_agents(agent_key),
    hub_key INT REFERENCES dim_hubs(hub_key),
    
    -- Location
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    h3_index VARCHAR(20),
    
    -- Details
    alert_message TEXT,
    alert_details JSONB,
    
    -- Resolution
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_by VARCHAR(50),
    acknowledged_at TIMESTAMP,
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP,
    resolution_notes TEXT,
    
    -- Processing
    notification_sent BOOLEAN DEFAULT FALSE,
    notification_channel VARCHAR(20)  -- SLACK, SMS, EMAIL
);
```

### fact_end_to_end_journey

```sql
-- Complete shipment journey from origin to delivery
-- Aggregated view connecting all modules

CREATE TABLE fact_end_to_end_journey (
    journey_id BIGINT PRIMARY KEY,
    shipment_key BIGINT REFERENCES dim_shipments(shipment_key),
    shipment_id VARCHAR(30) NOT NULL,
    
    -- Seller
    seller_key INT REFERENCES dim_sellers(seller_key),
    
    -- Customer
    customer_key INT REFERENCES dim_customers(customer_key),
    
    -- Route
    origin_hub_key INT REFERENCES dim_hubs(hub_key),
    destination_hub_key INT REFERENCES dim_hubs(hub_key),
    total_hubs INT,
    
    -- First mile (pickup)
    pickup_vehicle_key INT REFERENCES dim_vehicles(vehicle_key),
    pickup_timestamp TIMESTAMP,
    pickup_hub_arrival_timestamp TIMESTAMP,
    first_mile_hours DECIMAL(10, 2),
    
    -- Mid mile (hub to hub)
    num_line_haul_legs INT,
    line_haul_vehicle_keys INT[],  -- Array of vehicle keys
    mid_mile_hours DECIMAL(10, 2),
    total_hub_dwell_hours DECIMAL(10, 2),
    
    -- Last mile (delivery)
    delivery_agent_key INT REFERENCES dim_delivery_agents(agent_key),
    out_for_delivery_timestamp TIMESTAMP,
    delivery_timestamp TIMESTAMP,
    last_mile_hours DECIMAL(10, 2),
    delivery_attempts INT,
    
    -- Total journey
    total_journey_hours DECIMAL(10, 2),
    
    -- SLA
    promised_delivery_timestamp TIMESTAMP,
    sla_status VARCHAR(20),
    sla_variance_hours DECIMAL(10, 2),
    
    -- Final status
    final_status VARCHAR(20),  -- DELIVERED, RETURNED, LOST
    
    -- Bottleneck analysis
    longest_leg VARCHAR(20),  -- FIRST_MILE, MID_MILE, LAST_MILE, HUB_DWELL
    longest_leg_hours DECIMAL(10, 2),
    bottleneck_hub_key INT REFERENCES dim_hubs(hub_key)
);
```

---

## 4.8 DATA VOLUME ESTIMATES

| Table | Rows per Day | Rows per Month | Storage per Month |
|-------|--------------|----------------|-------------------|
| fact_vehicle_positions | 43M (500 vehicles × 8640 readings) | 1.3B | ~50 GB |
| fact_vehicle_telemetry | 14M | 430M | ~15 GB |
| fact_driving_events | 50K | 1.5M | ~500 MB |
| fact_shipment_events | 500K | 15M | ~5 GB |
| fact_agent_positions | 29M (1000 agents × 2880 readings) | 870M | ~30 GB |
| fact_delivery_attempts | 50K | 1.5M | ~500 MB |

**Total estimated storage:** ~100 GB per month for a mid-sized operation

---

This completes Part 2 (Data Model). Let me continue with Part 3 (Processing Logic and dbt Models).
