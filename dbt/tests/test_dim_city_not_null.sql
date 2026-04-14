-- Test: Ensure no null city names in dimension table
select *
from {{ ref('dim_city') }}
where city_name is null
