with latest as (
    select *
    from {{ ref('weather_latest_by_city') }}
),
cities as (
    select
        city_name,
        case
            when lower(city_name) = 'madrid' then 40.4168
            when lower(city_name) = 'barcelona' then 41.3874
            when lower(city_name) = 'valencia' then 39.4699
            when lower(city_name) = 'sevilla' then 37.3891
            when lower(city_name) = 'zaragoza' then 41.6488
            when lower(city_name) = 'málaga' then 36.7213
            when lower(city_name) = 'murcia' then 37.9922
            when lower(city_name) = 'palma' then 39.5696
            when lower(city_name) = 'bilbao' then 43.2630
            when lower(city_name) = 'alicante' then 38.3452
            when lower(city_name) = 'córdoba' then 37.8882
            when lower(city_name) = 'valladolid' then 41.6523
            when lower(city_name) = 'vigo' then 42.2406
            when lower(city_name) = 'gijón' then 43.5322
            when lower(city_name) = 'a coruña' then 43.3623
            when lower(city_name) = 'granada' then 37.1773
            when lower(city_name) = 'vitoria' then 42.8467
            when lower(city_name) = 'elche' then 38.2699
            when lower(city_name) = 'oviedo' then 43.3614
        end as lat,
        case
            when lower(city_name) = 'madrid' then -3.7038
            when lower(city_name) = 'barcelona' then 2.1686
            when lower(city_name) = 'valencia' then -0.3763
            when lower(city_name) = 'sevilla' then -5.9845
            when lower(city_name) = 'zaragoza' then -0.8891
            when lower(city_name) = 'málaga' then -4.4214
            when lower(city_name) = 'murcia' then -1.1307
            when lower(city_name) = 'palma' then 2.6502
            when lower(city_name) = 'bilbao' then -2.9350
            when lower(city_name) = 'alicante' then -0.4810
            when lower(city_name) = 'córdoba' then -4.7794
            when lower(city_name) = 'valladolid' then -4.7245
            when lower(city_name) = 'vigo' then -8.7207
            when lower(city_name) = 'gijón' then -5.6611
            when lower(city_name) = 'a coruña' then -8.4115
            when lower(city_name) = 'granada' then -3.5986
            when lower(city_name) = 'vitoria' then -2.6726
            when lower(city_name) = 'elche' then -0.7126
            when lower(city_name) = 'oviedo' then -5.8494
        end as lon,
        'ES' as country_code
    from latest
)

select
    l.city_name,
    c.lat,
    c.lon,
    l.api_timestamp,
    l.temperature_c,
    l.humidity_pct,
    l.wind_speed_ms,
    l.weather_main,
    l.weather_description,
    case
        when l.temperature_c >= 35 then 'Extreme heat'
        when l.wind_speed_ms >= 12 then 'High wind'
        else 'Normal'
    end as risk_type,
    case
        when l.temperature_c >= 35 then 3
        when l.wind_speed_ms >= 12 then 2
        else 1
    end as risk_level
from latest l
left join cities c
    on lower(l.city_name) = lower(c.city_name)
where l.temperature_c >= 35
   or l.wind_speed_ms >= 12