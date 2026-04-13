select
    city_name,
    date(api_timestamp) as weather_date,
    case
        when round(avg(clouds_pct),2) > 80 then 'Cloudy'
        when round(avg(rain_flag),2) = 1 then 'Rainy'
        when round(avg(temperature_c),2) > 30 then 'Hot'
        else 'Normal'
    end as weather_type
from {{ ref('stg_weather') }}
group by 1,2