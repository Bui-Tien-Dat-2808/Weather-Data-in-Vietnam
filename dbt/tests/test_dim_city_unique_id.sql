-- Test: Check for unique city IDs
select city_id
from {{ ref('dim_city') }}
group by city_id
having count(*) > 1
