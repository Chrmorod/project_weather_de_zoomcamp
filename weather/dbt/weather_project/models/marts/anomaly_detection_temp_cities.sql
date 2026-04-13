with stats as (
    select
        city_name,
        round(avg(temperature_c),2) as mean_temp,
        stddev(temperature_c) as std_temp
    from {{ ref('stg_weather') }}
    group by 1
)

select w.*
from {{ ref('stg_weather') }} w
join stats s using (city_name)
where abs(w.temperature_c - s.mean_temp) > 2 * s.std_temp