{{
    config(
        materialized='view',
        tags=['staging', 'fleet']
    )
}}

/*
    Staging model for vehicle telemetry (OBD-II data).

    - Standardizes telemetry readings
    - Adds health indicators
*/

with source as (
    select * from {{ source('bronze', 'vehicle_telemetry') }}
),

cleaned as (
    select
        -- Primary identifiers
        event_id,
        vehicle_id,

        -- Timestamps
        cast(timestamp as timestamp) as event_timestamp,
        cast(timestamp as date) as event_date,

        -- Engine metrics
        engine_rpm,
        engine_temp_c as engine_temp_celsius,
        oil_pressure_psi,
        coolant_temp_c as coolant_temp_celsius,

        -- Fuel & battery
        fuel_level_pct,
        battery_voltage,

        -- Usage metrics
        odometer_km,
        engine_hours,

        -- Health indicators
        case
            when engine_temp_c > 100 then 'OVERHEATING'
            when engine_temp_c < 60 and engine_rpm > 1000 then 'COLD_RUNNING'
            else 'NORMAL'
        end as engine_temp_status,

        case
            when oil_pressure_psi < 20 then 'LOW'
            when oil_pressure_psi > 80 then 'HIGH'
            else 'NORMAL'
        end as oil_pressure_status,

        case
            when battery_voltage < 12.0 then 'LOW'
            when battery_voltage > 14.5 then 'OVERCHARGING'
            else 'NORMAL'
        end as battery_status,

        case
            when engine_rpm > 5000 then true
            else false
        end as is_high_rpm,

        -- Ingestion metadata
        current_timestamp as dbt_loaded_at

    from source
)

select * from cleaned
