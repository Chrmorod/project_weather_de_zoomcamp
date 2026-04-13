select
    city_name,
    round(avg(wind_speed_ms),2) as avg_wind
from {{ ref('stg_weather') }}
group by 1
order by avg_wind desc