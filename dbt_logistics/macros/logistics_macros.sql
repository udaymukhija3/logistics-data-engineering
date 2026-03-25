-- dbt_logistics/macros/h3_index.sql

{% macro generate_surrogate_key(columns) %}
    md5(
        concat_ws(
            '||',
            {% for column in columns -%}
                coalesce(cast({{ column }} as varchar), '__dbt_null__')
                {%- if not loop.last %}, {% endif -%}
            {%- endfor %}
        )
    )
{% endmacro %}


{% macro date_spine(datepart, start_date, end_date) %}
    {% if datepart != 'day' %}
        {{ exceptions.raise_compiler_error('This project only implements day-level date_spine for DuckDB.') }}
    {% endif %}

    with recursive generated_dates as (
        select cast({{ start_date }} as date) as date_day
        union all
        select date_day + interval '1 day'
        from generated_dates
        where date_day < cast({{ end_date }} as date)
    )
    select * from generated_dates
{% endmacro %}

{% macro h3_index(lat_column, lng_column, resolution=9) %}
    {#- 
    Generate H3 index from latitude and longitude.
    Note: DuckDB doesn't have native H3 support, so this is a placeholder.
    In production, you would use a UDF or pre-compute H3 in Spark.
    -#}
    
    concat(
        'h3_',
        cast({{ resolution }} as varchar),
        '_',
        cast(round({{ lat_column }}, 2) as varchar),
        '_',
        cast(round({{ lng_column }}, 2) as varchar)
    )
{% endmacro %}


-- dbt_logistics/macros/calculate_distance.sql

{% macro haversine_distance(lat1, lon1, lat2, lon2) %}
    {#- 
    Calculate distance in kilometers between two points using Haversine formula.
    -#}
    
    6371 * 2 * asin(sqrt(
        power(sin(radians(({{ lat2 }} - {{ lat1 }}) / 2)), 2) +
        cos(radians({{ lat1 }})) * cos(radians({{ lat2 }})) *
        power(sin(radians(({{ lon2 }} - {{ lon1 }}) / 2)), 2)
    ))
{% endmacro %}


-- dbt_logistics/macros/sla_status.sql

{% macro sla_status(promised_date, actual_date, is_delivered) %}
    {#- 
    Calculate SLA status based on promised and actual delivery dates.
    -#}
    
    case
        when {{ is_delivered }} = false then 'PENDING'
        when {{ actual_date }} is null then 'PENDING'
        when {{ actual_date }} <= {{ promised_date }} then 'MET'
        else 'BREACHED'
    end
{% endmacro %}


{% macro sla_variance_days(promised_date, actual_date) %}
    {#- 
    Calculate variance in days (negative = early, positive = late).
    -#}
    
    datediff('day', {{ promised_date }}, {{ actual_date }})
{% endmacro %}


-- dbt_logistics/macros/time_bands.sql

{% macro time_band(hour_column) %}
    {#- 
    Categorize hour into time bands for analysis.
    -#}
    
    case
        when {{ hour_column }} between 0 and 5 then 'NIGHT'
        when {{ hour_column }} between 6 and 8 then 'EARLY_MORNING'
        when {{ hour_column }} between 9 and 11 then 'MORNING'
        when {{ hour_column }} between 12 and 14 then 'AFTERNOON'
        when {{ hour_column }} between 15 and 17 then 'LATE_AFTERNOON'
        when {{ hour_column }} between 18 and 20 then 'EVENING'
        else 'NIGHT'
    end
{% endmacro %}


{% macro is_peak_hour(hour_column) %}
    {#- 
    Check if hour is peak traffic hour.
    -#}
    
    ({{ hour_column }} between 8 and 10) or ({{ hour_column }} between 17 and 20)
{% endmacro %}


-- dbt_logistics/macros/delivery_slot.sql

{% macro delivery_slot(hour_column) %}
    {#- 
    Categorize hour into delivery slots.
    -#}
    
    case
        when {{ hour_column }} between 9 and 12 then 'MORNING'
        when {{ hour_column }} between 12 and 17 then 'AFTERNOON'
        when {{ hour_column }} between 17 and 21 then 'EVENING'
        else 'OFF_HOURS'
    end
{% endmacro %}


-- dbt_logistics/macros/validate_coordinates.sql

{% macro is_valid_india_coordinates(lat_column, lng_column) %}
    {#- 
    Check if coordinates are within India bounds.
    -#}
    
    (
        {{ lat_column }} between {{ var('india_lat_min') }} and {{ var('india_lat_max') }}
        and {{ lng_column }} between {{ var('india_lng_min') }} and {{ var('india_lng_max') }}
    )
{% endmacro %}


-- dbt_logistics/macros/event_category.sql

{% macro shipment_event_category(event_type_column) %}
    {#- 
    Categorize shipment events into mile categories.
    -#}
    
    case
        when {{ event_type_column }} in ('CREATED', 'PICKUP_SCHEDULED', 'PICKED_UP') then 'FIRST_MILE'
        when {{ event_type_column }} in ('HUB_ARRIVED', 'HUB_INSCAN', 'HUB_SORTED', 'HUB_OUTSCAN', 'HUB_DEPARTED', 'IN_TRANSIT') then 'MID_MILE'
        when {{ event_type_column }} in ('OUT_FOR_DELIVERY', 'DELIVERY_ATTEMPTED', 'DELIVERED', 'DELIVERY_FAILED') then 'LAST_MILE'
        when {{ event_type_column }} in ('RETURNED_TO_ORIGIN', 'LOST', 'DAMAGED') then 'EXCEPTION'
        else 'OTHER'
    end
{% endmacro %}


-- dbt_logistics/macros/driving_event_severity.sql

{% macro speeding_severity(speed_kmh_column) %}
    {#- 
    Categorize speeding severity.
    -#}
    
    case
        when {{ speed_kmh_column }} > 120 then 'CRITICAL'
        when {{ speed_kmh_column }} > 100 then 'HIGH'
        when {{ speed_kmh_column }} > 90 then 'MEDIUM'
        when {{ speed_kmh_column }} > 80 then 'LOW'
        else null
    end
{% endmacro %}


-- dbt_logistics/macros/generate_date_spine.sql

{% macro generate_date_spine(start_date, end_date) %}
    {{ date_spine('day', start_date, end_date) }}
{% endmacro %}
