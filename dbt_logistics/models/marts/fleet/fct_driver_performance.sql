{{
    config(
        materialized='table',
        tags=['marts', 'facts', 'fleet']
    )
}}

/*
    Fact: Driver daily performance.

    Grain: One row per driver per day.
    Contains driver performance metrics for fleet management.
*/

with driver_metrics as (
    select * from {{ ref('int_driver_daily_metrics') }}
),

enriched as (
    select
        -- Keys
        {{ generate_surrogate_key(['driver_id', 'event_date']) }} as performance_key,
        driver_id,
        vehicle_id,
        event_date as date_key,

        -- Activity measures
        active_hours,
        position_count,
        moving_positions,
        stopped_positions,
        idle_positions,
        utilization_rate,

        -- Speed measures
        avg_speed_kmh,
        max_speed_kmh,

        -- Fuel measures
        fuel_consumed_pct,

        -- Safety measures
        total_alerts,
        speeding_alerts,
        harsh_braking_alerts,
        harsh_accel_alerts,
        alert_severity_score,
        safety_score,

        -- Performance tier
        case
            when safety_score >= 90 and utilization_rate >= 0.7 then 'EXCELLENT'
            when safety_score >= 75 and utilization_rate >= 0.5 then 'GOOD'
            when safety_score >= 60 then 'AVERAGE'
            else 'NEEDS_IMPROVEMENT'
        end as performance_rating,

        dbt_loaded_at

    from driver_metrics
)

select * from enriched
