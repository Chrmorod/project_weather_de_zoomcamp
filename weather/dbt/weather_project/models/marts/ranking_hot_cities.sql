select
    city_name,
    round(avg(temp_max_c),2) as avg_temp_max
from {{ ref('stg_weather') }}
group by 1
order by avg_temp desc