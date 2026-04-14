-- Dimension table: City information
-- Each city should appear only once
{{ config(
    materialized='table',
    tags=['dimension', 'daily'],
    indexes=[
        {'columns': ['city_name'], 'type': 'btree'}
    ]
) }}

select
    row_number() over (order by city) as city_id,
    city as city_name,
    'Vietnam' as country,
    null as latitude,
    null as longitude,
    current_timestamp as created_at
from (
    select distinct city
    from {{ ref('stg_weather_data') }}
    where city is not null
) cities
