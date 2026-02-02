{{
    config(
        materialized='table',
        tags=['marts', 'dimensions', 'common']
    )
}}

/*
    Dimension: Hub/Location dimension table.

    Master data for logistics hubs.
*/

with hub_data as (
    -- Static hub master data
    select * from (
        values
            ('HUB_DEL_01', 'Delhi Hub', 'Delhi', 'North', 28.5505, 77.2506, 'MEGA', 500, true),
            ('HUB_MUM_01', 'Mumbai Hub', 'Mumbai', 'West', 19.0330, 72.8520, 'MEGA', 450, true),
            ('HUB_BLR_01', 'Bangalore Hub', 'Bangalore', 'South', 13.0100, 77.5500, 'MEGA', 400, true),
            ('HUB_CHN_01', 'Chennai Hub', 'Chennai', 'South', 13.0600, 80.2100, 'MEGA', 350, true),
            ('HUB_HYD_01', 'Hyderabad Hub', 'Hyderabad', 'South', 17.4400, 78.3800, 'MEGA', 380, true),
            ('HUB_KOL_01', 'Kolkata Hub', 'Kolkata', 'East', 22.5726, 88.3639, 'MEGA', 320, true),
            ('HUB_PUN_01', 'Pune Hub', 'Pune', 'West', 18.5204, 73.8567, 'REGIONAL', 200, true),
            ('HUB_AMD_01', 'Ahmedabad Hub', 'Ahmedabad', 'West', 23.0225, 72.5714, 'REGIONAL', 180, true),
            ('HUB_JAI_01', 'Jaipur Hub', 'Jaipur', 'North', 26.9124, 75.7873, 'REGIONAL', 150, true),
            ('HUB_LKO_01', 'Lucknow Hub', 'Lucknow', 'North', 26.8467, 80.9462, 'REGIONAL', 140, true)
    ) as t(hub_id, hub_name, city, region, latitude, longitude, hub_type, capacity_daily, is_active)
)

select
    hub_id,
    hub_name,
    city,
    region,
    latitude,
    longitude,
    hub_type,
    capacity_daily,
    is_active,

    -- Derived attributes
    case hub_type
        when 'MEGA' then 1
        when 'REGIONAL' then 2
        when 'LOCAL' then 3
        else 4
    end as hub_tier,

    case region
        when 'North' then 'IN-N'
        when 'South' then 'IN-S'
        when 'East' then 'IN-E'
        when 'West' then 'IN-W'
        else 'IN-OT'
    end as region_code,

    current_timestamp as dbt_loaded_at

from hub_data
