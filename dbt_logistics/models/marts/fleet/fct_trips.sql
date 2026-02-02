{{
    config(
        materialized='table',
        tags=['marts', 'facts', 'fleet']
    )
}}

/*
    Fact: Vehicle trips.

    Grain: One row per vehicle trip.
    Contains trip metrics for fleet analytics.
*/

with trips as (
    select * from {{ ref('int_vehicle_trips') }}
),

-- Add surrogate keys and enrich
enriched as (
    select
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['trip_id']) }} as trip_key,
        trip_id,
        vehicle_id,
        driver_id,

        -- Date keys for dimension joins
        trip_date as date_key,

        -- Trip details
        vehicle_type,
        trip_type,
        trip_start_time,
        trip_end_time,

        -- Location
        start_latitude,
        start_longitude,
        end_latitude,
        end_longitude,
        straight_line_distance_km,

        -- Measures
        total_distance_km,
        trip_duration_minutes,
        avg_speed_kmh,
        max_speed_kmh,
        fuel_consumed_pct,
        stop_count,
        position_count,

        -- Calculated measures
        case
            when straight_line_distance_km > 0
            then total_distance_km / straight_line_distance_km
            else null
        end as route_circuity,

        case
            when trip_duration_minutes > 0
            then total_distance_km / (trip_duration_minutes / 60)
            else null
        end as effective_speed_kmh,

        case
            when total_distance_km > 0 and fuel_consumed_pct > 0
            then total_distance_km / fuel_consumed_pct
            else null
        end as fuel_efficiency_km_per_pct,

        dbt_loaded_at

    from trips
)

select * from enriched
