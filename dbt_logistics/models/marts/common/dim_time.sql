{{
    config(
        materialized='table',
        tags=['marts', 'dimensions', 'common']
    )
}}

/*
    Dimension: Time/Date dimension table.

    Provides date attributes for analytics.
*/

with date_spine as (
    -- Generate dates for analysis range
    {{ date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2026-12-31' as date)"
    ) }}
),

enriched as (
    select
        date_day as date_key,
        date_day as full_date,

        -- Date parts
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        extract(month from date_day) as month,
        extract(week from date_day) as week_of_year,
        extract(day from date_day) as day_of_month,
        extract(dow from date_day) as day_of_week,
        extract(doy from date_day) as day_of_year,

        -- Derived
        strftime(date_day, '%Y-%m') as year_month,
        strftime(date_day, '%Y') || '-Q' || cast(extract(quarter from date_day) as varchar) as year_quarter,
        strftime(date_day, '%B') as month_name,
        strftime(date_day, '%b') as month_name_short,
        strftime(date_day, '%A') as day_name,
        strftime(date_day, '%a') as day_name_short,

        -- Flags
        case
            when extract(dow from date_day) in (0, 6) then true
            else false
        end as is_weekend,

        case
            when extract(dow from date_day) between 1 and 5 then true
            else false
        end as is_weekday,

        -- Fiscal (assuming April start for India)
        case
            when extract(month from date_day) >= 4
            then extract(year from date_day)
            else extract(year from date_day) - 1
        end as fiscal_year,

        case
            when extract(month from date_day) >= 4
            then extract(month from date_day) - 3
            else extract(month from date_day) + 9
        end as fiscal_month

    from date_spine
)

select * from enriched
