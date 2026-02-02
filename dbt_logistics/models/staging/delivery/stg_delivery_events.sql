{{
    config(
        materialized='view',
        tags=['staging', 'delivery']
    )
}}

/*
    Staging model for delivery attempt and completion events.

    - Cleans and standardizes delivery event data
    - Categorizes outcomes and failures
*/

with source as (
    select * from {{ source('bronze', 'delivery_events') }}
),

cleaned as (
    select
        -- Primary identifiers
        event_id,
        agent_id,
        agent_name,
        order_id,
        shipment_id,

        -- Timestamps
        cast(timestamp as timestamp) as event_timestamp,
        cast(timestamp as date) as event_date,

        -- Event details
        event_type,

        -- Customer context
        customer_id,
        delivery_address,
        zone_id,

        -- Location
        delivery_lat as delivery_latitude,
        delivery_lng as delivery_longitude,

        -- Payment
        is_cod,
        cod_amount,
        cod_collected,
        payment_mode,

        -- Delivery tracking
        attempt_number,
        failure_reason,

        -- Proof of delivery
        pod_type,

        -- Customer feedback
        customer_rating,

        -- Operational metrics
        time_at_location_seconds,

        -- Outcome classification
        case
            when event_type = 'DELIVERED' then 'SUCCESS'
            when event_type = 'DELIVERY_FAILED' then 'FAILED'
            when event_type = 'DELIVERY_ATTEMPTED' then 'ATTEMPTED'
            else 'OTHER'
        end as delivery_outcome,

        -- Is first attempt success?
        case
            when event_type = 'DELIVERED' and attempt_number = 1 then true
            else false
        end as is_first_attempt_success,

        -- Failure categorization
        case
            when failure_reason in ('CUSTOMER_NOT_AVAILABLE', 'ACCESS_RESTRICTED')
                then 'CUSTOMER_ISSUE'
            when failure_reason in ('WRONG_ADDRESS')
                then 'ADDRESS_ISSUE'
            when failure_reason in ('CUSTOMER_REFUSED', 'PAYMENT_ISSUE')
                then 'REFUSAL'
            when failure_reason in ('DAMAGED_PACKAGE')
                then 'PACKAGE_ISSUE'
            else null
        end as failure_category,

        -- Time features
        extract(hour from cast(timestamp as timestamp)) as hour_of_day,
        extract(dow from cast(timestamp as timestamp)) as day_of_week,

        -- Time slot classification
        case
            when extract(hour from cast(timestamp as timestamp)) between 9 and 12 then 'MORNING'
            when extract(hour from cast(timestamp as timestamp)) between 12 and 15 then 'AFTERNOON'
            when extract(hour from cast(timestamp as timestamp)) between 15 and 18 then 'EVENING'
            when extract(hour from cast(timestamp as timestamp)) between 18 and 21 then 'NIGHT'
            else 'OFF_HOURS'
        end as delivery_time_slot,

        -- Ingestion metadata
        current_timestamp as dbt_loaded_at

    from source
)

select * from cleaned
