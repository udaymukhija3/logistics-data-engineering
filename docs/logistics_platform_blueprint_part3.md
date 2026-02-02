# UNIFIED LOGISTICS DATA PLATFORM
## Part 3: Processing Logic, dbt Models, and Project Structure

---

# SECTION 5: STREAM PROCESSING (SPARK STRUCTURED STREAMING)

## 5.1 Vehicle Position Processing

```python
# src/streaming/vehicle_position_processor.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import h3

# UDF for H3 indexing
@udf(StringType())
def lat_lng_to_h3(lat, lng, resolution=9):
    if lat is None or lng is None:
        return None
    try:
        return h3.geo_to_h3(lat, lng, resolution)
    except:
        return None

# India bounds for validation
INDIA_LAT_MIN, INDIA_LAT_MAX = 8.0, 37.0
INDIA_LNG_MIN, INDIA_LNG_MAX = 68.0, 97.5

def process_vehicle_positions(spark: SparkSession, kafka_bootstrap: str):
    """
    Real-time processing of vehicle GPS positions.
    """
    
    # Define schema for GPS messages
    gps_schema = StructType([
        StructField("vehicle_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("speed_kmh", DoubleType()),
        StructField("heading_degrees", IntegerType()),
        StructField("driver_id", StringType()),
    ])
    
    # Read from Kafka
    raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", "vehicle_positions") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON
    parsed = raw_stream \
        .select(from_json(col("value").cast("string"), gps_schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
    
    # Validate and enrich
    enriched = parsed \
        .withColumn("is_valid_lat", 
            (col("latitude") >= INDIA_LAT_MIN) & (col("latitude") <= INDIA_LAT_MAX)) \
        .withColumn("is_valid_lng",
            (col("longitude") >= INDIA_LNG_MIN) & (col("longitude") <= INDIA_LNG_MAX)) \
        .withColumn("is_valid_speed",
            (col("speed_kmh") >= 0) & (col("speed_kmh") <= 200)) \
        .withColumn("is_valid_position",
            col("is_valid_lat") & col("is_valid_lng") & col("is_valid_speed")) \
        .withColumn("h3_index_res9", lat_lng_to_h3(col("latitude"), col("longitude"), lit(9))) \
        .withColumn("processing_timestamp", current_timestamp())
    
    # Write to Bronze layer (all data, including invalid)
    bronze_query = enriched \
        .writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/data/checkpoints/vehicle_positions_bronze") \
        .partitionBy("event_date") \
        .start("/data/bronze/vehicle_positions")
    
    return bronze_query


def detect_driving_events(spark: SparkSession, kafka_bootstrap: str):
    """
    Detect driving events (speeding, harsh braking) from GPS stream.
    Uses stateful processing to compare with previous position.
    """
    
    # Read positions
    positions = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", "vehicle_positions") \
        .load()
    
    # Parse and add window
    parsed = positions \
        .select(from_json(col("value").cast("string"), gps_schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
        .withWatermark("event_timestamp", "1 minute")
    
    # Detect speeding (speed > 80 km/h or > 100 on highways)
    speeding_events = parsed \
        .filter(col("speed_kmh") > 80) \
        .withColumn("event_type", lit("SPEEDING")) \
        .withColumn("severity", 
            when(col("speed_kmh") > 120, "CRITICAL")
            .when(col("speed_kmh") > 100, "HIGH")
            .when(col("speed_kmh") > 90, "MEDIUM")
            .otherwise("LOW"))
    
    # Write speeding events to Kafka alerts topic
    speeding_query = speeding_events \
        .select(to_json(struct("*")).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("topic", "alerts") \
        .option("checkpointLocation", "/data/checkpoints/speeding_alerts") \
        .start()
    
    return speeding_query
```

## 5.2 Shipment Event Processing

```python
# src/streaming/shipment_event_processor.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

def process_shipment_events(spark: SparkSession, kafka_bootstrap: str):
    """
    Process shipment scan events and update current state.
    """
    
    event_schema = StructType([
        StructField("event_id", StringType()),
        StructField("shipment_id", StringType()),
        StructField("awb_number", StringType()),
        StructField("event_type", StringType()),
        StructField("timestamp", StringType()),
        StructField("hub_id", StringType()),
        StructField("hub_city", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("worker_id", StringType()),
        StructField("metadata", StringType()),
    ])
    
    # Read from Kafka
    raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", "shipment_events") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse
    parsed = raw_stream \
        .select(from_json(col("value").cast("string"), event_schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
        .withWatermark("event_timestamp", "5 minutes")
    
    # Write all events to Bronze (append-only event store)
    bronze_query = parsed \
        .writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/data/checkpoints/shipment_events_bronze") \
        .start("/data/bronze/shipment_events")
    
    return bronze_query


def update_shipment_status(spark: SparkSession, kafka_bootstrap: str):
    """
    Update shipment_status table based on events.
    Uses foreachBatch to perform upserts.
    """
    
    def upsert_to_status(batch_df, batch_id):
        """Upsert batch to shipment_status Delta table."""
        
        if batch_df.isEmpty():
            return
        
        # Get latest event per shipment in this batch
        latest_events = batch_df \
            .groupBy("shipment_id") \
            .agg(
                max_by(struct("*"), col("event_timestamp")).alias("latest")
            ) \
            .select("latest.*")
        
        # Load target table
        status_table = DeltaTable.forPath(spark, "/data/silver/shipment_status")
        
        # Upsert
        status_table.alias("target").merge(
            latest_events.alias("source"),
            "target.shipment_id = source.shipment_id"
        ).whenMatchedUpdate(set={
            "current_status": col("source.event_type"),
            "current_hub_id": col("source.hub_id"),
            "last_event_id": col("source.event_id"),
            "last_event_type": col("source.event_type"),
            "last_event_timestamp": col("source.event_timestamp"),
            "current_latitude": col("source.latitude"),
            "current_longitude": col("source.longitude"),
            "updated_at": current_timestamp()
        }).whenNotMatchedInsert(values={
            "shipment_id": col("source.shipment_id"),
            "current_status": col("source.event_type"),
            "current_hub_id": col("source.hub_id"),
            "last_event_id": col("source.event_id"),
            "last_event_type": col("source.event_type"),
            "last_event_timestamp": col("source.event_timestamp"),
            "current_latitude": col("source.latitude"),
            "current_longitude": col("source.longitude"),
            "created_at": current_timestamp(),
            "updated_at": current_timestamp()
        }).execute()
    
    # Stream with foreachBatch
    parsed = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", "shipment_events") \
        .load() \
        .select(from_json(col("value").cast("string"), event_schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
    
    query = parsed \
        .writeStream \
        .foreachBatch(upsert_to_status) \
        .option("checkpointLocation", "/data/checkpoints/shipment_status_upsert") \
        .start()
    
    return query


def detect_stuck_shipments(spark: SparkSession):
    """
    Batch job to detect stuck shipments (no event in 24+ hours).
    Run every hour via Airflow.
    """
    
    status_df = spark.read.format("delta").load("/data/silver/shipment_status")
    
    stuck = status_df \
        .filter(~col("current_status").isin(["DELIVERED", "RETURNED", "LOST"])) \
        .withColumn("hours_since_last_event",
            (unix_timestamp(current_timestamp()) - unix_timestamp(col("last_event_timestamp"))) / 3600) \
        .filter(col("hours_since_last_event") > 24) \
        .withColumn("is_stuck", lit(True))
    
    # Generate alerts
    alerts = stuck \
        .select(
            lit("SHIPMENT").alias("module"),
            lit("STUCK_PACKAGE").alias("alert_type"),
            when(col("hours_since_last_event") > 72, "CRITICAL")
                .when(col("hours_since_last_event") > 48, "HIGH")
                .otherwise("WARNING").alias("severity"),
            col("shipment_id").alias("entity_id"),
            col("current_hub_id").alias("hub_id"),
            col("hours_since_last_event"),
            current_timestamp().alias("alert_timestamp")
        )
    
    # Write alerts
    alerts.write.format("delta").mode("append").save("/data/silver/alerts")
    
    return stuck.count()
```

## 5.3 Delivery Agent Processing

```python
# src/streaming/delivery_processor.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def process_delivery_events(spark: SparkSession, kafka_bootstrap: str):
    """
    Process delivery attempts and outcomes.
    """
    
    delivery_schema = StructType([
        StructField("event_id", StringType()),
        StructField("agent_id", StringType()),
        StructField("order_id", StringType()),
        StructField("shipment_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("result", StringType()),
        StructField("failure_reason", StringType()),
        StructField("time_at_stop_seconds", IntegerType()),
        StructField("pod_type", StringType()),
        StructField("cod_collected", DoubleType()),
        StructField("customer_rating", IntegerType()),
    ])
    
    raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", "delivery_events") \
        .load()
    
    parsed = raw_stream \
        .select(from_json(col("value").cast("string"), delivery_schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("h3_index", lat_lng_to_h3(col("latitude"), col("longitude"), lit(9)))
    
    # Write to Bronze
    bronze_query = parsed \
        .writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/data/checkpoints/delivery_events_bronze") \
        .start("/data/bronze/delivery_events")
    
    # Also generate alerts for failed deliveries
    failures = parsed.filter(col("result") == "FAILED")
    
    failure_alerts = failures \
        .select(
            lit("LAST_MILE").alias("module"),
            lit("DELIVERY_FAILED").alias("alert_type"),
            lit("WARNING").alias("severity"),
            col("order_id").alias("entity_id"),
            col("agent_id"),
            col("failure_reason"),
            col("event_timestamp").alias("alert_timestamp")
        )
    
    alert_query = failure_alerts \
        .select(to_json(struct("*")).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("topic", "alerts") \
        .option("checkpointLocation", "/data/checkpoints/delivery_failure_alerts") \
        .start()
    
    return bronze_query, alert_query
```

---

# SECTION 6: BATCH PROCESSING (SPARK JOBS)

## 6.1 Trip Reconstruction

```python
# src/batch/trip_reconstruction.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def reconstruct_trips(spark: SparkSession, date: str):
    """
    Reconstruct trips from GPS points.
    A trip = continuous movement with start and end.
    """
    
    # Load positions for date
    positions = spark.read.format("delta") \
        .load("/data/bronze/vehicle_positions") \
        .filter(col("event_date") == date) \
        .filter(col("is_valid_position") == True)
    
    # Window for detecting trip boundaries
    vehicle_window = Window.partitionBy("vehicle_id").orderBy("event_timestamp")
    
    # Calculate time gap between positions
    with_gaps = positions \
        .withColumn("prev_timestamp", lag("event_timestamp").over(vehicle_window)) \
        .withColumn("gap_minutes", 
            (unix_timestamp(col("event_timestamp")) - unix_timestamp(col("prev_timestamp"))) / 60) \
        .withColumn("prev_speed", lag("speed_kmh").over(vehicle_window))
    
    # Trip boundary: gap > 30 min OR was stationary and now moving
    with_boundaries = with_gaps \
        .withColumn("is_trip_start",
            (col("gap_minutes") > 30) | 
            (col("gap_minutes").isNull()) |
            ((col("prev_speed") < 2) & (col("speed_kmh") >= 5)))
    
    # Assign trip IDs
    with_trip_ids = with_boundaries \
        .withColumn("trip_id", 
            sum(col("is_trip_start").cast("int")).over(vehicle_window))
    
    # Aggregate to trips
    trips = with_trip_ids \
        .groupBy("vehicle_id", "driver_id", "trip_id") \
        .agg(
            min("event_timestamp").alias("start_timestamp"),
            max("event_timestamp").alias("end_timestamp"),
            first("latitude").alias("start_latitude"),
            first("longitude").alias("start_longitude"),
            last("latitude").alias("end_latitude"),
            last("longitude").alias("end_longitude"),
            avg("speed_kmh").alias("avg_speed_kmh"),
            max("speed_kmh").alias("max_speed_kmh"),
            count("*").alias("position_count")
        ) \
        .withColumn("duration_minutes",
            (unix_timestamp(col("end_timestamp")) - unix_timestamp(col("start_timestamp"))) / 60)
    
    # Filter out very short trips (< 5 min, < 5 positions)
    valid_trips = trips \
        .filter((col("duration_minutes") >= 5) & (col("position_count") >= 5))
    
    # Write to Silver
    valid_trips.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("start_date") \
        .save("/data/silver/trips")
    
    return valid_trips.count()
```

## 6.2 Shipment Journey Reconstruction

```python
# src/batch/journey_reconstruction.py

def reconstruct_shipment_journeys(spark: SparkSession, date: str):
    """
    Reconstruct complete journey for each shipment.
    """
    
    # Load events for delivered shipments
    events = spark.read.format("delta") \
        .load("/data/bronze/shipment_events") \
        .filter(to_date(col("event_timestamp")) <= date)
    
    # Get shipments delivered on this date
    delivered = events \
        .filter(col("event_type") == "DELIVERED") \
        .filter(to_date(col("event_timestamp")) == date) \
        .select("shipment_id")
    
    # Get all events for these shipments
    shipment_events = events.join(delivered, "shipment_id")
    
    # Order events per shipment
    window = Window.partitionBy("shipment_id").orderBy("event_timestamp")
    
    ordered = shipment_events \
        .withColumn("event_sequence", row_number().over(window)) \
        .withColumn("next_event_timestamp", lead("event_timestamp").over(window))
    
    # Extract hub stops (HUB_ARRIVED followed by HUB_DEPARTED)
    hub_stops = ordered \
        .filter(col("event_type") == "HUB_ARRIVED") \
        .withColumn("departure_timestamp", 
            lead("event_timestamp").over(window)) \
        .withColumn("dwell_time_hours",
            (unix_timestamp(col("departure_timestamp")) - unix_timestamp(col("event_timestamp"))) / 3600)
    
    # Build journey
    journeys = hub_stops \
        .withColumn("hop_sequence", row_number().over(
            Window.partitionBy("shipment_id").orderBy("event_timestamp"))) \
        .select(
            "shipment_id",
            "hop_sequence",
            "hub_id",
            "hub_city",
            col("event_timestamp").alias("arrival_timestamp"),
            "departure_timestamp",
            "dwell_time_hours",
            (col("dwell_time_hours") > 12).alias("is_bottleneck")
        )
    
    # Write
    journeys.write \
        .format("delta") \
        .mode("append") \
        .save("/data/silver/shipment_journeys")
    
    return journeys.count()
```

## 6.3 Agent Shift Aggregation

```python
# src/batch/agent_shift_aggregation.py

def aggregate_agent_shifts(spark: SparkSession, date: str):
    """
    Calculate daily shift metrics per agent.
    """
    
    # Load delivery attempts
    deliveries = spark.read.format("delta") \
        .load("/data/bronze/delivery_events") \
        .filter(to_date(col("event_timestamp")) == date)
    
    # Load agent positions (for distance calculation)
    positions = spark.read.format("delta") \
        .load("/data/bronze/agent_positions") \
        .filter(to_date(col("event_timestamp")) == date)
    
    # Aggregate deliveries per agent
    delivery_agg = deliveries \
        .groupBy("agent_id") \
        .agg(
            count("*").alias("total_attempts"),
            sum(when(col("result") == "DELIVERED", 1).otherwise(0)).alias("orders_delivered"),
            sum(when(col("result") == "FAILED", 1).otherwise(0)).alias("orders_failed"),
            sum(col("cod_collected")).alias("total_cod_collected"),
            avg("customer_rating").alias("avg_customer_rating"),
            avg("time_at_stop_seconds").alias("avg_time_at_stop_seconds"),
            min("event_timestamp").alias("first_delivery_time"),
            max("event_timestamp").alias("last_delivery_time")
        )
    
    # Calculate success rate
    shifts = delivery_agg \
        .withColumn("success_rate", 
            col("orders_delivered") / col("total_attempts") * 100) \
        .withColumn("shift_date", lit(date)) \
        .withColumn("active_hours",
            (unix_timestamp(col("last_delivery_time")) - unix_timestamp(col("first_delivery_time"))) / 3600) \
        .withColumn("deliveries_per_hour",
            col("orders_delivered") / col("active_hours"))
    
    # Write
    shifts.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("shift_date") \
        .save("/data/silver/agent_shifts")
    
    return shifts.count()
```

---

# SECTION 7: dbt MODELS

## 7.1 dbt Project Structure

```
dbt_logistics/
├── dbt_project.yml
├── profiles.yml
├── packages.yml
│
├── models/
│   ├── staging/
│   │   ├── stg_vehicle_positions.sql
│   │   ├── stg_vehicle_telemetry.sql
│   │   ├── stg_shipment_events.sql
│   │   ├── stg_delivery_attempts.sql
│   │   ├── stg_agent_positions.sql
│   │   └── schema.yml
│   │
│   ├── intermediate/
│   │   ├── int_trips_enriched.sql
│   │   ├── int_shipment_journeys.sql
│   │   ├── int_delivery_metrics.sql
│   │   └── schema.yml
│   │
│   └── marts/
│       ├── core/
│       │   ├── dim_vehicles.sql
│       │   ├── dim_drivers.sql
│       │   ├── dim_hubs.sql
│       │   ├── dim_agents.sql
│       │   ├── dim_shipments.sql
│       │   └── dim_geography.sql
│       │
│       ├── fleet/
│       │   ├── fct_trips.sql
│       │   ├── fct_driving_events.sql
│       │   ├── fct_vehicle_utilization.sql
│       │   └── rpt_fleet_daily.sql
│       │
│       ├── shipments/
│       │   ├── fct_shipment_events.sql
│       │   ├── fct_sla_tracking.sql
│       │   ├── fct_hub_throughput.sql
│       │   └── rpt_sla_daily.sql
│       │
│       └── delivery/
│           ├── fct_delivery_attempts.sql
│           ├── fct_agent_shifts.sql
│           ├── fct_area_performance.sql
│           └── rpt_delivery_daily.sql
│
├── tests/
│   ├── generic/
│   │   └── test_valid_coordinates.sql
│   └── singular/
│       └── test_sla_calculation.sql
│
├── macros/
│   ├── h3_index.sql
│   ├── calculate_distance.sql
│   └── sla_status.sql
│
└── seeds/
    ├── ref_hubs.csv
    ├── ref_sla_definitions.csv
    └── ref_holiday_calendar.csv
```

## 7.2 Sample dbt Models

### Staging: stg_shipment_events.sql

```sql
-- models/staging/stg_shipment_events.sql

{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('bronze', 'shipment_events') }}
),

renamed as (
    select
        event_id,
        shipment_id,
        awb_number,
        event_type,
        event_timestamp,
        hub_id,
        hub_city,
        latitude,
        longitude,
        worker_id,
        device_id,
        metadata,
        
        -- Derived
        date(event_timestamp) as event_date,
        hour(event_timestamp) as event_hour,
        
        -- H3 index
        {{ h3_index('latitude', 'longitude', 9) }} as h3_index_res9,
        
        -- Event category
        case
            when event_type in ('CREATED', 'PICKUP_SCHEDULED', 'PICKED_UP') then 'FIRST_MILE'
            when event_type in ('HUB_ARRIVED', 'HUB_DEPARTED', 'IN_TRANSIT') then 'MID_MILE'
            when event_type in ('OUT_FOR_DELIVERY', 'DELIVERED', 'DELIVERY_FAILED') then 'LAST_MILE'
            else 'OTHER'
        end as event_category
        
    from source
    where event_timestamp is not null
)

select * from renamed
```

### Intermediate: int_shipment_journeys.sql

```sql
-- models/intermediate/int_shipment_journeys.sql

{{
    config(
        materialized='table',
        partition_by={
            "field": "delivery_date",
            "data_type": "date"
        }
    )
}}

with events as (
    select * from {{ ref('stg_shipment_events') }}
),

hub_arrivals as (
    select
        shipment_id,
        hub_id,
        event_timestamp as arrival_timestamp,
        lead(event_timestamp) over (
            partition by shipment_id 
            order by event_timestamp
        ) as next_event_timestamp,
        lead(event_type) over (
            partition by shipment_id 
            order by event_timestamp
        ) as next_event_type,
        row_number() over (
            partition by shipment_id 
            order by event_timestamp
        ) as hop_sequence
    from events
    where event_type = 'HUB_ARRIVED'
),

with_dwell_time as (
    select
        shipment_id,
        hub_id,
        hop_sequence,
        arrival_timestamp,
        case 
            when next_event_type = 'HUB_DEPARTED' then next_event_timestamp
            else null
        end as departure_timestamp,
        datediff('hour', arrival_timestamp, next_event_timestamp) as dwell_time_hours
    from hub_arrivals
)

select
    {{ dbt_utils.generate_surrogate_key(['shipment_id', 'hop_sequence']) }} as journey_id,
    shipment_id,
    hop_sequence,
    hub_id,
    arrival_timestamp,
    departure_timestamp,
    dwell_time_hours,
    dwell_time_hours > 12 as is_bottleneck,
    hop_sequence = 1 as is_first_hub,
    date(arrival_timestamp) as delivery_date
from with_dwell_time
```

### Mart: fct_sla_tracking.sql

```sql
-- models/marts/shipments/fct_sla_tracking.sql

{{
    config(
        materialized='incremental',
        unique_key='shipment_id',
        incremental_strategy='merge'
    )
}}

with shipments as (
    select * from {{ ref('dim_shipments') }}
),

sla_definitions as (
    select * from {{ ref('ref_sla_definitions') }}
),

shipment_events as (
    select * from {{ ref('stg_shipment_events') }}
),

delivery_info as (
    select
        shipment_id,
        min(case when event_type = 'CREATED' then event_timestamp end) as created_at,
        min(case when event_type = 'DELIVERED' then event_timestamp end) as delivered_at,
        max(case when event_type in ('DELIVERED', 'RETURNED', 'LOST') then event_type end) as final_status
    from shipment_events
    group by 1
),

final as (
    select
        s.shipment_id,
        s.origin_city,
        s.destination_city,
        s.service_type,
        
        date(d.created_at) as created_date,
        s.promised_delivery_date,
        date(d.delivered_at) as actual_delivery_date,
        
        sla.promised_days,
        datediff('day', date(d.created_at), date(d.delivered_at)) as actual_days,
        
        datediff('day', s.promised_delivery_date, date(d.delivered_at)) as variance_days,
        
        case
            when d.delivered_at is null then 'PENDING'
            when date(d.delivered_at) <= s.promised_delivery_date then 'MET'
            else 'BREACHED'
        end as sla_status,
        
        d.final_status,
        d.delivered_at is not null as is_delivered,
        date(d.delivered_at) <= s.promised_delivery_date as is_on_time
        
    from shipments s
    left join delivery_info d on s.shipment_id = d.shipment_id
    left join sla_definitions sla 
        on s.origin_city = sla.origin_city 
        and s.destination_city = sla.destination_city
        and s.service_type = sla.service_type
)

select * from final

{% if is_incremental() %}
where created_date >= (select max(created_date) from {{ this }}) - interval '3 days'
{% endif %}
```

### Mart: fct_area_performance.sql

```sql
-- models/marts/delivery/fct_area_performance.sql

{{
    config(
        materialized='table',
        partition_by={
            "field": "date",
            "data_type": "date"
        }
    )
}}

with deliveries as (
    select * from {{ ref('stg_delivery_attempts') }}
),

aggregated as (
    select
        h3_index_res9 as geo_key,
        date(event_timestamp) as date,
        
        -- Volume
        count(*) as total_attempts,
        sum(case when result = 'DELIVERED' then 1 else 0 end) as successful_deliveries,
        sum(case when result = 'FAILED' then 1 else 0 end) as failed_deliveries,
        
        -- Rates
        round(100.0 * sum(case when result = 'DELIVERED' then 1 else 0 end) / count(*), 2) as success_rate,
        
        -- Timing
        avg(time_at_stop_seconds) as avg_time_at_stop_seconds,
        
        -- Failures breakdown
        sum(case when failure_reason = 'CUSTOMER_NOT_AVAILABLE' then 1 else 0 end) as failures_customer_not_available,
        sum(case when failure_reason = 'WRONG_ADDRESS' then 1 else 0 end) as failures_wrong_address,
        sum(case when failure_reason = 'ACCESS_ISSUE' then 1 else 0 end) as failures_access_issue,
        sum(case when failure_reason = 'REFUSED' then 1 else 0 end) as failures_refused,
        
        -- Customer satisfaction
        avg(customer_rating) as avg_customer_rating
        
    from deliveries
    group by 1, 2
)

select
    {{ dbt_utils.generate_surrogate_key(['geo_key', 'date']) }} as area_perf_id,
    *
from aggregated
```

## 7.3 dbt Tests (schema.yml)

```yaml
# models/marts/shipments/schema.yml

version: 2

models:
  - name: fct_sla_tracking
    description: "SLA tracking for all shipments"
    columns:
      - name: shipment_id
        description: "Unique shipment identifier"
        tests:
          - unique
          - not_null
      - name: sla_status
        description: "SLA status: MET, BREACHED, PENDING"
        tests:
          - accepted_values:
              values: ['MET', 'BREACHED', 'PENDING']
      - name: variance_days
        tests:
          - dbt_utils.expression_is_true:
              expression: "variance_days >= -30 and variance_days <= 30"
              
  - name: fct_hub_throughput
    description: "Daily hub throughput metrics"
    columns:
      - name: hub_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_hubs')
              field: hub_id
      - name: packages_received
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0"
```

---

# SECTION 8: PROJECT STRUCTURE

```
unified-logistics-platform/
│
├── README.md
├── requirements.txt
├── Makefile
├── docker-compose.yml
├── .env.example
├── .gitignore
│
├── infrastructure/
│   ├── kafka/
│   │   ├── docker-compose.kafka.yml
│   │   └── topics.sh
│   ├── spark/
│   │   └── docker-compose.spark.yml
│   ├── airflow/
│   │   ├── docker-compose.airflow.yml
│   │   └── airflow.cfg
│   └── storage/
│       └── docker-compose.minio.yml
│
├── src/
│   ├── __init__.py
│   ├── config.py
│   │
│   ├── simulators/
│   │   ├── __init__.py
│   │   ├── vehicle_gps_simulator.py
│   │   ├── shipment_event_simulator.py
│   │   ├── delivery_agent_simulator.py
│   │   └── run_all_simulators.py
│   │
│   ├── streaming/
│   │   ├── __init__.py
│   │   ├── vehicle_position_processor.py
│   │   ├── shipment_event_processor.py
│   │   ├── delivery_processor.py
│   │   └── alert_processor.py
│   │
│   ├── batch/
│   │   ├── __init__.py
│   │   ├── trip_reconstruction.py
│   │   ├── journey_reconstruction.py
│   │   ├── agent_shift_aggregation.py
│   │   ├── sla_calculation.py
│   │   └── hub_throughput.py
│   │
│   ├── quality/
│   │   ├── __init__.py
│   │   ├── expectations/
│   │   │   ├── vehicle_positions.json
│   │   │   ├── shipment_events.json
│   │   │   └── delivery_attempts.json
│   │   └── run_quality_checks.py
│   │
│   └── utils/
│       ├── __init__.py
│       ├── h3_utils.py
│       ├── geofence_utils.py
│       └── alerting.py
│
├── dbt_logistics/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   ├── tests/
│   ├── macros/
│   └── seeds/
│
├── dags/
│   ├── daily_batch_processing.py
│   ├── hourly_quality_checks.py
│   ├── dbt_transformations.py
│   └── sla_reporting.py
│
├── tests/
│   ├── test_simulators.py
│   ├── test_streaming.py
│   ├── test_batch.py
│   └── test_quality.py
│
├── notebooks/
│   ├── 01_data_exploration.ipynb
│   ├── 02_fleet_analytics.ipynb
│   ├── 03_shipment_analytics.ipynb
│   └── 04_delivery_analytics.ipynb
│
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   ├── setup_guide.md
│   └── api_reference.md
│
└── monitoring/
    ├── dashboards/
    │   ├── fleet_dashboard.json
    │   ├── shipment_dashboard.json
    │   └── delivery_dashboard.json
    └── alerts/
        └── alert_rules.yml
```

---

# SECTION 9: TIMELINE

| Week | Focus | Deliverables |
|------|-------|--------------|
| **Week 1** | Infrastructure + Simulators | Docker Compose setup, Kafka topics, all 3 simulators generating data |
| **Week 2** | Stream Processing | Spark Streaming jobs for all 3 modules, Bronze layer populating |
| **Week 3** | Batch Processing | Trip/journey reconstruction, aggregations, Silver layer |
| **Week 4** | dbt + Data Model | All staging, intermediate, mart models with tests |
| **Week 5** | Quality + Orchestration | Great Expectations, Airflow DAGs, end-to-end pipeline |
| **Week 6** | Documentation + Polish | README, architecture docs, notebooks, final testing |

**Total: 6 weeks**

---

# SECTION 10: WHAT THIS DEMONSTRATES

| Skill Category | Specific Skills |
|----------------|-----------------|
| **Data Sourcing** | Kafka producers, CDC concepts, API simulation |
| **Data Modeling** | Dimensional modeling, star schema, SCD Type 2 |
| **Stream Processing** | Spark Structured Streaming, stateful processing, windowing |
| **Batch Processing** | Spark batch jobs, trip reconstruction, aggregations |
| **Orchestration** | Airflow DAGs, dependencies, retries, alerting |
| **Data Quality** | Great Expectations, dbt tests, validation |
| **Transformations** | dbt, incremental models, macros |
| **Geospatial** | H3 indexing, geofencing, PostGIS |
| **Storage** | Delta Lake, partitioning, ACID transactions |
| **DevOps** | Docker Compose, multi-service orchestration |

---

# SECTION 11: INTERVIEW TALKING POINTS

**"Walk me through this system"**
> "It's a unified logistics data platform with three modules: fleet tracking, shipment tracking, and last-mile delivery. GPS data from vehicles streams through Kafka at 10-second intervals, processed by Spark Streaming for geofence detection and driving events. Shipment scan events flow through a separate stream, updating shipment status in real-time and detecting stuck packages. Delivery agent tracking follows similar patterns. Batch jobs reconstruct trips and journeys daily, and dbt models build the dimensional warehouse. Everything is orchestrated by Airflow with quality gates using Great Expectations."

**"Why did you combine three problems?"**
> "Because that's how logistics actually works. A shipment travels on a truck (fleet), through hubs (shipment events), and is delivered by an agent (last-mile). Separating them would mean duplicate dimensions and miss the end-to-end view. Companies like Delhivery need to trace a shipment from origin to delivery across all these touchpoints."

**"What was the hardest part?"**
> "Correlating data across modules. A shipment is on a specific truck during transit — joining those requires matching timestamps and locations carefully. Also, reconstructing trips from raw GPS points requires handling noise, gaps, and edge cases like brief stops at traffic lights versus actual delivery stops."

**"How would you scale this?"**
> "Kafka scales horizontally with partitions. Spark scales with more executors. I'd move from local Delta Lake to Databricks or AWS Glue Data Catalog. For the warehouse, I'd use ClickHouse or BigQuery for analytical queries at scale. The architecture is already designed for this — no fundamental changes needed."

---

**Dataset:** All simulated (you control the volume and patterns)

**This is a comprehensive logistics data platform that covers data sourcing, modeling, streaming, batch processing, and orchestration — exactly what Data Engineering roles demand.**
