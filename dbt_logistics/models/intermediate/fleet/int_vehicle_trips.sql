{{
    config(
        materialized='table',
        tags=['intermediate', 'fleet']
    )
}}

/*
    Intermediate model: Vehicle trips with metrics.

    Aggregates GPS positions into trips and calculates:
    - Trip duration and distance
    - Average and max speeds
    - Stop counts and idle time
    - Route efficiency
*/

with positions as (
    select * from {{ ref('stg_vehicle_positions') }}
),

-- Window for ordering positions within each vehicle
position_ordered as (
    select
        *,
        lag(event_timestamp) over (
            partition by vehicle_id
            order by event_timestamp
        ) as prev_timestamp,
        lag(latitude) over (
            partition by vehicle_id
            order by event_timestamp
        ) as prev_lat,
        lag(longitude) over (
            partition by vehicle_id
            order by event_timestamp
        ) as prev_lng,
        lag(vehicle_state) over (
            partition by vehicle_id
            order by event_timestamp
        ) as prev_state
    from positions
    where is_valid_location = true
),

-- Calculate time gap to identify trip boundaries
with_gaps as (
    select
        *,
        extract(epoch from (event_timestamp - prev_timestamp)) / 60 as gap_minutes,
        case
            when prev_timestamp is null then true
            when extract(epoch from (event_timestamp - prev_timestamp)) / 60 > 30 then true
            when vehicle_state = 'MOVING' and prev_state in ('IDLE', 'STOPPED') then true
            else false
        end as is_trip_start
    from position_ordered
),

-- Assign trip IDs
with_trip_ids as (
    select
        *,
        vehicle_id || '_' || to_char(event_date, 'YYYYMMDD') || '_' ||
            lpad(cast(sum(case when is_trip_start then 1 else 0 end)
                over (partition by vehicle_id order by event_timestamp) as varchar), 3, '0')
            as trip_id_generated
    from with_gaps
),

-- Calculate segment distances using Haversine approximation
with_distances as (
    select
        *,
        case
            when prev_lat is not null and prev_lng is not null then
                -- Simplified Haversine for small distances
                111.0 * sqrt(
                    power(latitude - prev_lat, 2) +
                    power((longitude - prev_lng) * cos(radians(latitude)), 2)
                )
            else 0
        end as segment_distance_km
    from with_trip_ids
),

-- Aggregate trips
trips as (
    select
        coalesce(trip_id, trip_id_generated) as trip_id,
        vehicle_id,
        first_value(driver_id) over (
            partition by coalesce(trip_id, trip_id_generated)
            order by event_timestamp
        ) as driver_id,
        first_value(vehicle_type) over (
            partition by coalesce(trip_id, trip_id_generated)
            order by event_timestamp
        ) as vehicle_type,

        min(event_timestamp) as trip_start_time,
        max(event_timestamp) as trip_end_time,
        min(event_date) as trip_date,

        -- Start/end locations
        first_value(latitude) over (
            partition by coalesce(trip_id, trip_id_generated)
            order by event_timestamp
        ) as start_latitude,
        first_value(longitude) over (
            partition by coalesce(trip_id, trip_id_generated)
            order by event_timestamp
        ) as start_longitude,
        last_value(latitude) over (
            partition by coalesce(trip_id, trip_id_generated)
            order by event_timestamp
            rows between unbounded preceding and unbounded following
        ) as end_latitude,
        last_value(longitude) over (
            partition by coalesce(trip_id, trip_id_generated)
            order by event_timestamp
            rows between unbounded preceding and unbounded following
        ) as end_longitude,

        -- Metrics
        sum(segment_distance_km) as total_distance_km,
        count(*) as position_count,
        avg(speed_kmh) as avg_speed_kmh,
        max(speed_kmh) as max_speed_kmh,

        -- Fuel
        max(fuel_level_pct) as fuel_start_pct,
        min(fuel_level_pct) as fuel_end_pct,

        -- Stop analysis
        sum(case when vehicle_state = 'STOPPED' then 1 else 0 end) as stop_count

    from with_distances
    group by
        coalesce(trip_id, trip_id_generated),
        vehicle_id,
        driver_id,
        vehicle_type,
        latitude,
        longitude,
        event_timestamp
)

-- Final trip summary
select distinct
    trip_id,
    vehicle_id,
    driver_id,
    vehicle_type,
    trip_start_time,
    trip_end_time,
    trip_date,
    start_latitude,
    start_longitude,
    end_latitude,
    end_longitude,
    total_distance_km,
    position_count,

    -- Duration in minutes
    extract(epoch from (trip_end_time - trip_start_time)) / 60 as trip_duration_minutes,

    avg_speed_kmh,
    max_speed_kmh,
    fuel_start_pct,
    fuel_end_pct,
    fuel_start_pct - fuel_end_pct as fuel_consumed_pct,
    stop_count,

    -- Straight-line distance
    111.0 * sqrt(
        power(end_latitude - start_latitude, 2) +
        power((end_longitude - start_longitude) * cos(radians(start_latitude)), 2)
    ) as straight_line_distance_km,

    -- Trip classification
    case
        when 111.0 * sqrt(
            power(end_latitude - start_latitude, 2) +
            power((end_longitude - start_longitude) * cos(radians(start_latitude)), 2)
        ) < 5 then 'LOCAL'
        when 111.0 * sqrt(
            power(end_latitude - start_latitude, 2) +
            power((end_longitude - start_longitude) * cos(radians(start_latitude)), 2)
        ) < 50 then 'SHORT_HAUL'
        when 111.0 * sqrt(
            power(end_latitude - start_latitude, 2) +
            power((end_longitude - start_longitude) * cos(radians(start_latitude)), 2)
        ) < 200 then 'MEDIUM_HAUL'
        else 'LONG_HAUL'
    end as trip_type,

    current_timestamp as dbt_loaded_at

from trips
where position_count >= 5
    and extract(epoch from (trip_end_time - trip_start_time)) / 60 >= 5  -- At least 5 minutes
