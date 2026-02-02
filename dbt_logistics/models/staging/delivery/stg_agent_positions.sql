{{
    config(
        materialized='view',
        tags=['staging', 'delivery']
    )
}}

/*
    Staging model for delivery agent GPS positions.

    - Cleans and standardizes agent position data
    - Adds stop detection and activity flags
*/

with source as (
    select * from {{ source('bronze', 'agent_positions') }}
),

cleaned as (
    select
        -- Primary identifiers
        event_id,
        agent_id,

        -- Timestamps
        cast(timestamp as timestamp) as event_timestamp,
        cast(timestamp as date) as event_date,

        -- Location
        latitude,
        longitude,
        accuracy_m as accuracy_meters,

        -- Movement
        speed_kmh,
        heading as heading_degrees,

        -- Agent context
        zone_id,
        vehicle_type,
        status as agent_status,

        -- Stop detection
        is_at_stop,
        current_order_id,

        -- Daily metrics (from simulator)
        pending_orders,
        completed_today as completed_deliveries,
        failed_today as failed_deliveries,

        -- Device info
        battery_pct as phone_battery_pct,

        -- Data quality flags
        case
            when latitude between {{ var('india_lat_min') }} and {{ var('india_lat_max') }}
                and longitude between {{ var('india_lng_min') }} and {{ var('india_lng_max') }}
            then true
            else false
        end as is_valid_location,

        -- Activity classification
        case
            when is_at_stop = true then 'STOPPED'
            when speed_kmh < 2 then 'IDLE'
            when speed_kmh < 10 then 'SLOW_MOVING'
            else 'MOVING'
        end as activity_state,

        -- Time features
        extract(hour from cast(timestamp as timestamp)) as hour_of_day,
        extract(dow from cast(timestamp as timestamp)) as day_of_week,

        -- Is working hours?
        case
            when extract(hour from cast(timestamp as timestamp)) between 8 and 21
                then true
            else false
        end as is_working_hours,

        -- Ingestion metadata
        current_timestamp as dbt_loaded_at

    from source
    where latitude is not null
        and longitude is not null
)

select * from cleaned
