{{
    config(
        materialized='table',
        tags=['intermediate', 'shipment']
    )
}}

/*
    Intermediate model: Hub throughput metrics.

    Calculates daily hub-level metrics:
    - Inbound/outbound volumes
    - Processing times
    - Bottleneck detection
*/

with events as (
    select * from {{ ref('stg_shipment_events') }}
),

-- Hub-level event aggregations
hub_daily_events as (
    select
        hub_id,
        hub_name,
        hub_city,
        event_date,

        -- Volume metrics
        count(distinct shipment_id) as unique_shipments,
        count(*) as total_events,

        -- Inbound (arrivals)
        sum(case when event_type = 'HUB_ARRIVED' then 1 else 0 end) as arrivals,
        sum(case when event_type = 'HUB_INSCAN' then 1 else 0 end) as inscans,

        -- Processing
        sum(case when event_type = 'HUB_SORTED' then 1 else 0 end) as sorted,

        -- Outbound
        sum(case when event_type = 'HUB_OUTSCAN' then 1 else 0 end) as outscans,
        sum(case when event_type = 'HUB_DEPARTED' then 1 else 0 end) as departures,

        -- Time distribution
        sum(case when hour_of_day between 6 and 11 then 1 else 0 end) as morning_events,
        sum(case when hour_of_day between 12 and 17 then 1 else 0 end) as afternoon_events,
        sum(case when hour_of_day between 18 and 23 then 1 else 0 end) as evening_events,
        sum(case when hour_of_day between 0 and 5 then 1 else 0 end) as night_events,

        -- Worker activity
        count(distinct worker_id) as unique_workers,
        count(distinct scanner_id) as unique_scanners

    from events
    where hub_id is not null
    group by hub_id, hub_name, hub_city, event_date
),

-- Calculate derived metrics
with_derived as (
    select
        *,

        -- Throughput ratio (outbound vs inbound)
        case
            when arrivals > 0
            then cast(departures as float) / arrivals
            else null
        end as throughput_ratio,

        -- Processing rate (sorted vs inscans)
        case
            when inscans > 0
            then cast(sorted as float) / inscans
            else null
        end as processing_rate,

        -- Busiest period
        case
            when morning_events >= afternoon_events
                and morning_events >= evening_events
                and morning_events >= night_events
            then 'MORNING'
            when afternoon_events >= evening_events
                and afternoon_events >= night_events
            then 'AFTERNOON'
            when evening_events >= night_events
            then 'EVENING'
            else 'NIGHT'
        end as peak_period,

        -- Backlog indicator (more arrivals than departures)
        case
            when arrivals > departures * 1.2 then true
            else false
        end as has_backlog

    from hub_daily_events
)

select
    *,

    -- Efficiency score
    case
        when throughput_ratio >= 0.95 then 'HIGH'
        when throughput_ratio >= 0.8 then 'MEDIUM'
        else 'LOW'
    end as efficiency_rating,

    current_timestamp as dbt_loaded_at

from with_derived
