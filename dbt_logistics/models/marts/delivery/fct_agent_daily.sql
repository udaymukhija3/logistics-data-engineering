{{
    config(
        materialized='table',
        tags=['marts', 'facts', 'delivery']
    )
}}

/*
    Fact: Agent daily performance.

    Grain: One row per delivery agent per day.
    Contains delivery agent performance metrics.
*/

with agent_shifts as (
    select * from {{ ref('int_agent_shifts') }}
),

enriched as (
    select
        -- Keys
        {{ generate_surrogate_key(['agent_id', 'shift_date']) }} as agent_daily_key,
        agent_id,
        zone_id,
        shift_date as date_key,

        -- Agent attributes
        vehicle_type,
        performance_tier,

        -- Shift measures
        shift_start_time,
        shift_end_time,
        shift_duration_hours,

        -- Delivery measures
        successful_deliveries,
        failed_attempts,
        final_failures,
        first_attempt_successes,

        -- Calculated KPIs
        deliveries_per_hour,
        delivery_success_rate,
        first_attempt_success_rate,

        -- COD measures
        cod_deliveries,
        total_cod_collected,

        -- Customer satisfaction
        avg_customer_rating,
        ratings_received,

        -- Operational measures
        avg_time_per_delivery_seconds,
        position_count,
        stop_count,
        avg_speed_kmh,

        -- Time distribution
        morning_deliveries,
        afternoon_deliveries,
        evening_deliveries,
        night_deliveries,

        -- Device health
        avg_battery_pct,
        min_battery_pct,

        -- Flags for aggregation
        case when successful_deliveries >= 30 then 1 else 0 end as is_productive_flag,
        case when delivery_success_rate >= 0.9 then 1 else 0 end as is_high_quality_flag,
        case when avg_customer_rating >= 4.5 then 1 else 0 end as is_highly_rated_flag,

        dbt_loaded_at

    from agent_shifts
)

select * from enriched
