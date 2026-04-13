select
    city_name,
    api_timestamp,
    temperature_c,
    humidity_pct,
    round((temperature_c - (0.55 - 0.0055 * humidity_pct) * (temperature_c - 14.5)),2) as comfort_index
from {{ ref('stg_weather') }}