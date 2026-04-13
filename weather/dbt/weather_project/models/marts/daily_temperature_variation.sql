select
    city_name,
    api_timestamp,
    temperature_c,
    temperature_c - lag(temperature_c) over (
        partition by city_name
        order by api_timestamp
    ) as temp_change
from {{ ref('stg_weather') }}