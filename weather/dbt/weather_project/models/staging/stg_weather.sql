select
    id,
    city_name,
    country_code,
    api_timestamp,
    ingestion_timestamp,
    temperature_c,
    feels_like_c,
    temp_min_c,
    temp_max_c,
    pressure_hpa,
    humidity_pct,
    wind_speed_ms,
    weather_main,
    weather_description,
    clouds_pct,
    sunrise_ts,
    sunset_ts,
    case
    when lower(weather_main) in ('rain', 'drizzle', 'thunderstorm') then 1
    else 0
end as rain_flag
from {{ source('weather_raw', 'weather_api') }}
where city_name is not null