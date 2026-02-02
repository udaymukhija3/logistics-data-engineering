{{
    config(
        materialized='table',
        tags=['intermediate', 'shipment']
    )
}}

/*
    Intermediate model: Shipment journeys.

    Reconstructs complete shipment journeys from events:
    - Journey timeline
    - Hub-by-hub tracking
    - SLA status calculation
    - Bottleneck identification
*/

with events as (
    select * from {{ ref('stg_shipment_events') }}
),

-- Order events within each shipment
ordered_events as (
    select
        *,
        row_number() over (
            partition by shipment_id
            order by event_timestamp
        ) as event_sequence,
        lag(event_timestamp) over (
            partition by shipment_id
            order by event_timestamp
        ) as prev_event_time,
        lag(event_type) over (
            partition by shipment_id
            order by event_timestamp
        ) as prev_event_type,
        lag(hub_id) over (
            partition by shipment_id
            order by event_timestamp
        ) as prev_hub_id
    from events
),

-- Calculate time between events
with_gaps as (
    select
        *,
        extract(epoch from (event_timestamp - prev_event_time)) / 3600 as hours_since_prev_event,
        case
            when extract(epoch from (event_timestamp - prev_event_time)) / 3600 > {{ var('stuck_shipment_hours') }}
            then true
            else false
        end as is_stuck_event
    from ordered_events
),

-- Journey summary per shipment
journey_summary as (
    select
        shipment_id,
        awb_number,
        seller_id,
        customer_id,
        origin_hub,
        destination_hub,
        max(is_cod) as is_cod,
        max(cod_amount) as cod_amount,
        max(route_hops) as route_hops,

        -- Timestamps
        min(event_timestamp) as journey_start_time,
        max(event_timestamp) as journey_end_time,
        min(event_date) as journey_start_date,
        max(promised_delivery_at) as promised_delivery_at,

        -- First and last events
        min(case when event_sequence = 1 then event_type end) as first_event_type,
        max(event_type) as last_event_type,

        -- Event counts
        count(*) as total_events,
        count(distinct hub_id) as hubs_visited,
        sum(case when event_category = 'HUB_OPERATION' then 1 else 0 end) as hub_events,
        max(delivery_attempts) as delivery_attempts,
        max(failure_reason) as last_failure_reason,

        -- Stuck detection
        sum(case when is_stuck_event then 1 else 0 end) as stuck_incidents,
        max(hours_since_prev_event) as max_gap_hours

    from with_gaps
    group by
        shipment_id, awb_number, seller_id, customer_id,
        origin_hub, destination_hub
),

-- Calculate journey metrics
with_metrics as (
    select
        *,

        -- Duration
        extract(epoch from (journey_end_time - journey_start_time)) / 3600 as journey_duration_hours,
        extract(epoch from (journey_end_time - journey_start_time)) / 86400 as journey_duration_days,

        -- SLA status
        case
            when last_event_type = 'DELIVERED' then
                case
                    when journey_end_time <= promised_delivery_at then 'MET'
                    else 'BREACHED'
                end
            when last_event_type in ('DELIVERY_FAILED', 'RETURNED_TO_ORIGIN', 'LOST', 'DAMAGED') then 'FAILED'
            else 'IN_PROGRESS'
        end as sla_status,

        -- SLA variance (positive = late)
        case
            when last_event_type = 'DELIVERED'
            then extract(epoch from (journey_end_time - promised_delivery_at)) / 3600
            else null
        end as sla_variance_hours,

        -- Journey outcome
        case
            when last_event_type = 'DELIVERED' then 'DELIVERED'
            when last_event_type = 'DELIVERY_FAILED' then 'FAILED'
            when last_event_type = 'RETURNED_TO_ORIGIN' then 'RETURNED'
            when stuck_incidents > 0 then 'STUCK'
            else 'IN_TRANSIT'
        end as journey_outcome

    from journey_summary
)

select
    *,

    -- Classification
    case
        when hubs_visited <= 2 then 'SIMPLE'
        when hubs_visited <= 4 then 'MODERATE'
        else 'COMPLEX'
    end as journey_complexity,

    case
        when delivery_attempts > 1 then true
        else false
    end as had_delivery_issues,

    case
        when route_hops <= 2 then true
        else false
    end as is_express,

    current_timestamp as dbt_loaded_at

from with_metrics
