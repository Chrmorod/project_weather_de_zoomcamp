select
    city_name,
    date(api_timestamp) as weather_date,
    max(temp_max_c) as max_temp,
    min(temp_min_c) as min_temp
from {{ ref('stg_weather') }}
group by 1,2