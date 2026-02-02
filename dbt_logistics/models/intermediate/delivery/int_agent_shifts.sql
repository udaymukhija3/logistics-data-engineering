{{
    config(
        materialized='table',
        tags=['intermediate', 'delivery']
    )
}}

/*
    Intermediate model: Agent daily shifts.

    Aggregates daily metrics per delivery agent:
    - Shift duration
    - Deliveries completed/failed
    - Performance metrics
*/

with positions as (
    select * from {{ ref('stg_agent_positions') }}
),

deliveries as (
    select * from {{ ref('stg_delivery_events') }}
),

-- Agent daily position summary
agent_daily_positions as (
    select
        agent_id,
        zone_id,
        vehicle_type,
        event_date,

        -- Shift timing
        min(event_timestamp) as shift_start_time,
        max(event_timestamp) as shift_end_time,

        -- Activity counts
        count(*) as position_count,
        sum(case when is_at_stop then 1 else 0 end) as stop_count,
        sum(case when activity_state = 'MOVING' then 1 else 0 end) as moving_positions,

        -- Speed metrics
        avg(speed_kmh) as avg_speed_kmh,
        max(speed_kmh) as max_speed_kmh,

        -- Device health
        avg(phone_battery_pct) as avg_battery_pct,
        min(phone_battery_pct) as min_battery_pct,

        -- Last known counts from simulator
        max(completed_deliveries) as completed_from_device,
        max(failed_deliveries) as failed_from_device

    from positions
    where is_valid_location = true
    group by agent_id, zone_id, vehicle_type, event_date
),

-- Agent daily delivery summary
agent_daily_deliveries as (
    select
        agent_id,
        event_date,

        -- Delivery counts
        count(*) as total_delivery_events,
        sum(case when event_type = 'DELIVERED' then 1 else 0 end) as successful_deliveries,
        sum(case when event_type = 'DELIVERY_ATTEMPTED' then 1 else 0 end) as failed_attempts,
        sum(case when event_type = 'DELIVERY_FAILED' then 1 else 0 end) as final_failures,

        -- First attempt success
        sum(case when is_first_attempt_success then 1 else 0 end) as first_attempt_successes,

        -- COD metrics
        sum(case when is_cod and event_type = 'DELIVERED' then 1 else 0 end) as cod_deliveries,
        sum(case when event_type = 'DELIVERED' then cod_collected else 0 end) as total_cod_collected,

        -- Time at stops
        avg(time_at_location_seconds) as avg_time_per_delivery_seconds,

        -- Customer satisfaction
        avg(customer_rating) as avg_customer_rating,
        count(customer_rating) as ratings_received,

        -- Time slots
        sum(case when delivery_time_slot = 'MORNING' then 1 else 0 end) as morning_deliveries,
        sum(case when delivery_time_slot = 'AFTERNOON' then 1 else 0 end) as afternoon_deliveries,
        sum(case when delivery_time_slot = 'EVENING' then 1 else 0 end) as evening_deliveries,
        sum(case when delivery_time_slot = 'NIGHT' then 1 else 0 end) as night_deliveries

    from deliveries
    group by agent_id, event_date
),

-- Combine position and delivery data
combined as (
    select
        p.agent_id,
        p.zone_id,
        p.vehicle_type,
        p.event_date as shift_date,

        -- Shift metrics
        p.shift_start_time,
        p.shift_end_time,
        extract(epoch from (p.shift_end_time - p.shift_start_time)) / 3600 as shift_duration_hours,

        -- Movement metrics
        p.position_count,
        p.stop_count,
        p.avg_speed_kmh,
        p.max_speed_kmh,
        p.avg_battery_pct,
        p.min_battery_pct,

        -- Delivery metrics
        coalesce(d.successful_deliveries, 0) as successful_deliveries,
        coalesce(d.failed_attempts, 0) as failed_attempts,
        coalesce(d.final_failures, 0) as final_failures,
        coalesce(d.first_attempt_successes, 0) as first_attempt_successes,
        coalesce(d.cod_deliveries, 0) as cod_deliveries,
        coalesce(d.total_cod_collected, 0) as total_cod_collected,
        d.avg_time_per_delivery_seconds,
        d.avg_customer_rating,
        coalesce(d.ratings_received, 0) as ratings_received,

        -- Time distribution
        coalesce(d.morning_deliveries, 0) as morning_deliveries,
        coalesce(d.afternoon_deliveries, 0) as afternoon_deliveries,
        coalesce(d.evening_deliveries, 0) as evening_deliveries,
        coalesce(d.night_deliveries, 0) as night_deliveries

    from agent_daily_positions p
    left join agent_daily_deliveries d
        on p.agent_id = d.agent_id
        and p.event_date = d.event_date
)

select
    *,

    -- Calculate KPIs
    case
        when shift_duration_hours > 0
        then successful_deliveries / shift_duration_hours
        else 0
    end as deliveries_per_hour,

    case
        when (successful_deliveries + failed_attempts + final_failures) > 0
        then cast(successful_deliveries as float) /
            (successful_deliveries + failed_attempts + final_failures)
        else null
    end as delivery_success_rate,

    case
        when successful_deliveries > 0
        then cast(first_attempt_successes as float) / successful_deliveries
        else null
    end as first_attempt_success_rate,

    -- Performance tier
    case
        when successful_deliveries >= 50 then 'TOP_PERFORMER'
        when successful_deliveries >= 30 then 'GOOD'
        when successful_deliveries >= 15 then 'AVERAGE'
        else 'BELOW_AVERAGE'
    end as performance_tier,

    current_timestamp as dbt_loaded_at

from combined
