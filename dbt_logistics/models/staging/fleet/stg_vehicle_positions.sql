{{
    config(
        materialized='view',
        tags=['staging', 'fleet']
    )
}}

/*
    Staging model for vehicle GPS positions.

    - Cleans and standardizes raw position data
    - Adds computed columns for analysis
    - Filters invalid records
*/

with source as (
    select * from {{ source('bronze', 'vehicle_positions') }}
),

cleaned as (
    select
        -- Primary identifiers
        event_id,
        vehicle_id,
        driver_id,
        vehicle_type,

        -- Timestamps
        cast(timestamp as timestamp) as event_timestamp,
        cast(timestamp as date) as event_date,

        -- Location
        latitude,
        longitude,
        altitude_m as altitude_meters,
        accuracy_m as accuracy_meters,

        -- Movement
        speed_kmh,
        heading as heading_degrees,

        -- Trip context
        trip_id,
        state as vehicle_state,

        -- Vehicle metrics
        fuel_level_pct,
        odometer_km,

        -- Data quality flags
        case
            when latitude between {{ var('india_lat_min') }} and {{ var('india_lat_max') }}
                and longitude between {{ var('india_lng_min') }} and {{ var('india_lng_max') }}
            then true
            else false
        end as is_valid_location,

        case
            when speed_kmh between 0 and 200 then true
            else false
        end as is_valid_speed,

        -- Time-based features
        extract(hour from cast(timestamp as timestamp)) as hour_of_day,
        extract(dow from cast(timestamp as timestamp)) as day_of_week,

        case
            when extract(hour from cast(timestamp as timestamp)) between 8 and 10
                or extract(hour from cast(timestamp as timestamp)) between 17 and 20
            then true
            else false
        end as is_peak_hour,

        -- Ingestion metadata
        current_timestamp as dbt_loaded_at

    from source
    where latitude is not null
        and longitude is not null
)

select * from cleaned
