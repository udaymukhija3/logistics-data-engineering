{{
    config(
        materialized='table',
        tags=['marts', 'facts', 'delivery']
    )
}}

/*
    Fact: Zone daily performance.

    Grain: One row per delivery zone per day.
    Aggregated zone-level delivery metrics.
*/

with agent_daily as (
    select * from {{ ref('fct_agent_daily') }}
),

zone_aggregated as (
    select
        zone_id,
        date_key,

        -- Agent counts
        count(distinct agent_id) as active_agents,
        sum(case when performance_tier = 'TOP_PERFORMER' then 1 else 0 end) as top_performers,

        -- Delivery volumes
        sum(successful_deliveries) as total_deliveries,
        sum(failed_attempts) as total_failed_attempts,
        sum(final_failures) as total_final_failures,
        sum(first_attempt_successes) as total_first_attempt_successes,

        -- COD volumes
        sum(cod_deliveries) as total_cod_deliveries,
        sum(total_cod_collected) as total_cod_amount,

        -- Averages
        avg(deliveries_per_hour) as avg_deliveries_per_hour,
        avg(delivery_success_rate) as avg_success_rate,
        avg(first_attempt_success_rate) as avg_first_attempt_rate,
        avg(avg_customer_rating) as avg_customer_rating,
        sum(ratings_received) as total_ratings,

        -- Shift metrics
        avg(shift_duration_hours) as avg_shift_hours,

        -- Time distribution
        sum(morning_deliveries) as morning_deliveries,
        sum(afternoon_deliveries) as afternoon_deliveries,
        sum(evening_deliveries) as evening_deliveries,
        sum(night_deliveries) as night_deliveries,

        max(dbt_loaded_at) as dbt_loaded_at

    from agent_daily
    group by zone_id, date_key
),

enriched as (
    select
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['zone_id', 'date_key']) }} as zone_daily_key,
        zone_id,
        date_key,

        -- Agent metrics
        active_agents,
        top_performers,
        case
            when active_agents > 0
            then cast(top_performers as float) / active_agents
            else null
        end as top_performer_ratio,

        -- Volume metrics
        total_deliveries,
        total_failed_attempts,
        total_final_failures,
        total_first_attempt_successes,

        -- Calculated zone KPIs
        case
            when active_agents > 0
            then cast(total_deliveries as float) / active_agents
            else null
        end as deliveries_per_agent,

        case
            when (total_deliveries + total_failed_attempts + total_final_failures) > 0
            then cast(total_deliveries as float) /
                (total_deliveries + total_failed_attempts + total_final_failures)
            else null
        end as zone_success_rate,

        -- COD metrics
        total_cod_deliveries,
        total_cod_amount,
        case
            when total_cod_deliveries > 0
            then total_cod_amount / total_cod_deliveries
            else null
        end as avg_cod_per_delivery,

        -- Quality metrics
        avg_deliveries_per_hour,
        avg_success_rate,
        avg_first_attempt_rate,
        avg_customer_rating,
        total_ratings,

        -- Operational
        avg_shift_hours,

        -- Time distribution
        morning_deliveries,
        afternoon_deliveries,
        evening_deliveries,
        night_deliveries,

        -- Peak time
        case
            when morning_deliveries >= afternoon_deliveries
                and morning_deliveries >= evening_deliveries
                and morning_deliveries >= night_deliveries
            then 'MORNING'
            when afternoon_deliveries >= evening_deliveries
                and afternoon_deliveries >= night_deliveries
            then 'AFTERNOON'
            when evening_deliveries >= night_deliveries
            then 'EVENING'
            else 'NIGHT'
        end as peak_delivery_time,

        -- Zone performance tier
        case
            when avg_success_rate >= 0.9 and avg_customer_rating >= 4.5 then 'EXCELLENT'
            when avg_success_rate >= 0.8 and avg_customer_rating >= 4.0 then 'GOOD'
            when avg_success_rate >= 0.7 then 'AVERAGE'
            else 'NEEDS_IMPROVEMENT'
        end as zone_performance_tier,

        dbt_loaded_at

    from zone_aggregated
)

select * from enriched
