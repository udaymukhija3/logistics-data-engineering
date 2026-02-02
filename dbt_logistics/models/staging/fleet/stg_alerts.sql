{{
    config(
        materialized='view',
        tags=['staging', 'fleet']
    )
}}

/*
    Staging model for driving alerts and events.

    - Standardizes alert data
    - Categorizes by severity and type
*/

with source as (
    select * from {{ source('bronze', 'alerts') }}
),

cleaned as (
    select
        -- Primary identifiers
        event_id,
        vehicle_id,
        driver_id,

        -- Timestamps
        cast(timestamp as timestamp) as event_timestamp,
        cast(timestamp as date) as event_date,

        -- Alert details
        event_type as alert_type,
        severity,

        -- Location
        latitude,
        longitude,

        -- Speeding details
        speed as actual_speed_kmh,
        speed_limit as speed_limit_kmh,
        overspeed_by as overspeed_kmh,

        -- Braking/acceleration
        deceleration_ms2,
        acceleration_ms2,

        -- Categorization
        case
            when event_type = 'SPEEDING' then 'SAFETY'
            when event_type in ('HARSH_BRAKING', 'HARSH_ACCELERATION') then 'DRIVING_BEHAVIOR'
            else 'OTHER'
        end as alert_category,

        -- Severity scoring (for aggregation)
        case severity
            when 'CRITICAL' then 4
            when 'HIGH' then 3
            when 'WARNING' then 2
            when 'INFO' then 1
            else 0
        end as severity_score,

        -- Time features
        extract(hour from cast(timestamp as timestamp)) as hour_of_day,

        -- Ingestion metadata
        current_timestamp as dbt_loaded_at

    from source
)

select * from cleaned
