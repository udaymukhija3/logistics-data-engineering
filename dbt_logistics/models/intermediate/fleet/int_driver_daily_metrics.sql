{{
    config(
        materialized='table',
        tags=['intermediate', 'fleet']
    )
}}

/*
    Intermediate model: Daily driver metrics.

    Aggregates daily performance metrics per driver:
    - Trips completed
    - Distance driven
    - Driving behavior (alerts)
    - Fuel efficiency
*/

with positions as (
    select * from {{ ref('stg_vehicle_positions') }}
),

alerts as (
    select * from {{ ref('stg_alerts') }}
),

-- Daily position summary
daily_positions as (
    select
        driver_id,
        vehicle_id,
        event_date,
        count(*) as position_count,
        sum(case when vehicle_state = 'MOVING' then 1 else 0 end) as moving_positions,
        sum(case when vehicle_state = 'STOPPED' then 1 else 0 end) as stopped_positions,
        sum(case when vehicle_state = 'IDLE' then 1 else 0 end) as idle_positions,
        avg(speed_kmh) as avg_speed_kmh,
        max(speed_kmh) as max_speed_kmh,
        min(fuel_level_pct) as min_fuel_pct,
        max(fuel_level_pct) as max_fuel_pct,
        min(event_timestamp) as first_position_time,
        max(event_timestamp) as last_position_time
    from positions
    where driver_id is not null
        and is_valid_location = true
    group by driver_id, vehicle_id, event_date
),

-- Daily alert summary
daily_alerts as (
    select
        driver_id,
        event_date,
        count(*) as total_alerts,
        sum(case when alert_type = 'SPEEDING' then 1 else 0 end) as speeding_alerts,
        sum(case when alert_type = 'HARSH_BRAKING' then 1 else 0 end) as harsh_braking_alerts,
        sum(case when alert_type = 'HARSH_ACCELERATION' then 1 else 0 end) as harsh_accel_alerts,
        sum(severity_score) as total_severity_score,
        max(severity) as max_severity
    from alerts
    where driver_id is not null
    group by driver_id, event_date
),

-- Combine
combined as (
    select
        p.driver_id,
        p.vehicle_id,
        p.event_date,

        -- Activity metrics
        p.position_count,
        p.moving_positions,
        p.stopped_positions,
        p.idle_positions,

        -- Calculate active time (approximate)
        extract(epoch from (p.last_position_time - p.first_position_time)) / 3600 as active_hours,

        -- Speed metrics
        p.avg_speed_kmh,
        p.max_speed_kmh,

        -- Fuel metrics
        p.max_fuel_pct - p.min_fuel_pct as fuel_consumed_pct,

        -- Alert metrics
        coalesce(a.total_alerts, 0) as total_alerts,
        coalesce(a.speeding_alerts, 0) as speeding_alerts,
        coalesce(a.harsh_braking_alerts, 0) as harsh_braking_alerts,
        coalesce(a.harsh_accel_alerts, 0) as harsh_accel_alerts,
        coalesce(a.total_severity_score, 0) as alert_severity_score,

        -- Safety score (100 - penalties)
        greatest(0, 100
            - (coalesce(a.speeding_alerts, 0) * 5)
            - (coalesce(a.harsh_braking_alerts, 0) * 3)
            - (coalesce(a.harsh_accel_alerts, 0) * 2)
        ) as safety_score

    from daily_positions p
    left join daily_alerts a
        on p.driver_id = a.driver_id
        and p.event_date = a.event_date
)

select
    *,
    -- Utilization rate (moving vs total)
    case
        when position_count > 0
        then cast(moving_positions as float) / position_count
        else 0
    end as utilization_rate,

    current_timestamp as dbt_loaded_at

from combined
