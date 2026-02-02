{{
    config(
        materialized='table',
        tags=['marts', 'facts', 'shipment']
    )
}}

/*
    Fact: Shipment journeys.

    Grain: One row per shipment.
    Contains complete shipment journey metrics for logistics analytics.
*/

with journeys as (
    select * from {{ ref('int_shipment_journeys') }}
),

hubs as (
    select * from {{ ref('dim_hubs') }}
),

enriched as (
    select
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['j.shipment_id']) }} as shipment_key,
        j.shipment_id,
        j.awb_number,

        -- Party keys
        j.seller_id,
        j.customer_id,

        -- Hub keys
        j.origin_hub as origin_hub_id,
        j.destination_hub as destination_hub_id,
        o.hub_name as origin_hub_name,
        o.city as origin_city,
        o.region as origin_region,
        d.hub_name as destination_hub_name,
        d.city as destination_city,
        d.region as destination_region,

        -- Date keys
        j.journey_start_date as date_key,

        -- Timestamps
        j.journey_start_time,
        j.journey_end_time,
        j.promised_delivery_at,

        -- Journey attributes
        j.journey_outcome,
        j.sla_status,
        j.journey_complexity,
        j.is_express,
        j.is_cod,

        -- Route measures
        j.route_hops,
        j.hubs_visited,
        j.total_events,

        -- Time measures (in hours)
        j.journey_duration_hours,
        j.sla_variance_hours,
        j.max_gap_hours,

        -- Quality measures
        j.stuck_incidents,
        j.delivery_attempts,
        j.had_delivery_issues,

        -- Financial measures
        j.cod_amount,

        -- Flags
        case when j.sla_status = 'MET' then 1 else 0 end as sla_met_flag,
        case when j.sla_status = 'BREACHED' then 1 else 0 end as sla_breached_flag,
        case when j.journey_outcome = 'DELIVERED' then 1 else 0 end as delivered_flag,
        case when j.journey_outcome = 'FAILED' then 1 else 0 end as failed_flag,
        case when j.journey_outcome = 'RETURNED' then 1 else 0 end as returned_flag,

        j.dbt_loaded_at

    from journeys j
    left join hubs o on j.origin_hub = o.hub_id
    left join hubs d on j.destination_hub = d.hub_id
)

select * from enriched
