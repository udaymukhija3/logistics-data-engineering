{{
    config(
        materialized='view',
        tags=['staging', 'shipment']
    )
}}

/*
    Staging model for shipment tracking events.

    - Cleans and standardizes shipment scan events
    - Categorizes events by journey stage
    - Adds SLA context
*/

with source as (
    select * from {{ source('bronze', 'shipment_events') }}
),

cleaned as (
    select
        -- Primary identifiers
        event_id,
        shipment_id,
        awb_number,

        -- Timestamps
        cast(timestamp as timestamp) as event_timestamp,
        cast(timestamp as date) as event_date,

        -- Event details
        event_type,

        -- Hub information
        hub_id,
        hub_name,
        hub_city,
        latitude as hub_latitude,
        longitude as hub_longitude,

        -- Parties
        seller_id,
        customer_id,

        -- Route information
        origin_hub,
        destination_hub,
        route_hops,
        current_hop,

        -- Package details
        weight_kg,
        is_cod,
        cod_amount,

        -- Delivery tracking
        cast(promised_delivery as timestamp) as promised_delivery_at,
        delivery_attempts,
        failure_reason,

        -- Operational details
        scanner_id,
        worker_id,

        -- Journey stage categorization
        case
            when event_type in ('CREATED', 'PICKUP_SCHEDULED', 'PICKED_UP')
                then 'FIRST_MILE'
            when event_type in ('HUB_ARRIVED', 'HUB_INSCAN', 'HUB_SORTED',
                               'HUB_OUTSCAN', 'HUB_DEPARTED', 'IN_TRANSIT')
                then 'MID_MILE'
            when event_type in ('OUT_FOR_DELIVERY', 'DELIVERY_ATTEMPTED',
                               'DELIVERED', 'DELIVERY_FAILED')
                then 'LAST_MILE'
            when event_type in ('RETURNED_TO_ORIGIN', 'LOST', 'DAMAGED')
                then 'EXCEPTION'
            else 'OTHER'
        end as journey_stage,

        -- Event category
        case
            when event_type like 'HUB_%' then 'HUB_OPERATION'
            when event_type like 'DELIVERY%' or event_type = 'DELIVERED' then 'DELIVERY'
            when event_type in ('CREATED', 'PICKUP_SCHEDULED', 'PICKED_UP') then 'PICKUP'
            when event_type = 'IN_TRANSIT' then 'TRANSIT'
            else 'OTHER'
        end as event_category,

        -- Is terminal event?
        case
            when event_type in ('DELIVERED', 'DELIVERY_FAILED', 'RETURNED_TO_ORIGIN', 'LOST', 'DAMAGED')
                then true
            else false
        end as is_terminal_event,

        -- Time features
        extract(hour from cast(timestamp as timestamp)) as hour_of_day,
        extract(dow from cast(timestamp as timestamp)) as day_of_week,

        -- Ingestion metadata
        current_timestamp as dbt_loaded_at

    from source
)

select * from cleaned
