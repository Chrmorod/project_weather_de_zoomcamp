with ranked as (
    select
        *,
        row_number() over (
            partition by city_name
            order by api_timestamp desc
        ) as rn
    from {{ ref('stg_weather') }}
)

select
    city_name,
    country_code,
    api_timestamp,
    temperature_c,
    feels_like_c,
    humidity_pct,
    pressure_hpa,
    wind_speed_ms,
    weather_main,
    weather_description,
    clouds_pct
from ranked
where rn = 1