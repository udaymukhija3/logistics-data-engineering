{{
    config(
        materialized='table',
        tags=['marts', 'facts', 'shipment']
    )
}}

/*
    Fact: Hub daily metrics.

    Grain: One row per hub per day.
    Contains hub throughput and efficiency metrics.
*/

with hub_metrics as (
    select * from {{ ref('int_hub_throughput') }}
),

hubs as (
    select * from {{ ref('dim_hubs') }}
),

enriched as (
    select
        -- Keys
        {{ generate_surrogate_key(['h.hub_id', 'h.event_date']) }} as hub_daily_key,
        h.hub_id,
        h.event_date as date_key,

        -- Hub attributes (from dimension)
        d.hub_name,
        d.city,
        d.region,
        d.hub_type,
        d.capacity_daily,

        -- Volume measures
        h.unique_shipments,
        h.total_events,
        h.arrivals,
        h.departures,
        h.inscans,
        h.outscans,
        h.sorted,

        -- Efficiency measures
        h.throughput_ratio,
        h.processing_rate,
        h.efficiency_rating,

        -- Capacity utilization
        case
            when d.capacity_daily > 0
            then cast(h.unique_shipments as float) / d.capacity_daily
            else null
        end as capacity_utilization,

        -- Time distribution
        h.morning_events,
        h.afternoon_events,
        h.evening_events,
        h.night_events,
        h.peak_period,

        -- Operational measures
        h.unique_workers,
        h.unique_scanners,

        -- Flags
        h.has_backlog,
        case when h.efficiency_rating = 'LOW' then 1 else 0 end as is_bottleneck_flag,

        h.dbt_loaded_at

    from hub_metrics h
    left join hubs d on h.hub_id = d.hub_id
)

select * from enriched
