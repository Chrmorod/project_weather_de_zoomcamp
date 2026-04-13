select
    city_name,
    date(api_timestamp) as weather_date,
    round(avg(temperature_c),2) as avg_temperature_c,
    round(min(temp_min_c),2) as min_temperature_c,
    round(max(temp_max_c),2) as max_temperature_c,
    round(avg(humidity_pct),2) as avg_humidity_pct,
    round(avg(pressure_hpa),2) as avg_pressure_hpa,
    round(avg(wind_speed_ms),2) as avg_wind_speed_ms
from {{ ref('stg_weather') }}
group by 1, 2