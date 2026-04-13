select *
from {{ ref('stg_weather') }}
where temperature_c > 35