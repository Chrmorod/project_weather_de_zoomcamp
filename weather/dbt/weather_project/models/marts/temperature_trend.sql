with base as (
    select
        city_name,
        api_timestamp,
        round(temperature_c,2) as temperature_c
    from {{ ref('stg_weather') }}
)

select
    city_name,
    api_timestamp,
    temperature_c,
    avg(temperature_c) over (
        partition by city_name
        order by api_timestamp
        rows between 6 preceding and current row
    ) as temp_7d_avg
from base