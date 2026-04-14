-- Fact table: Weather measurements
-- One row per city per measurement time
{{ config(
    materialized='table',
    tags=['fact', 'daily'],
    indexes=[
        {'columns': ['city_id', 'measurement_timestamp'], 'type': 'btree'},
        {'columns': ['measurement_timestamp'], 'type': 'btree'}
    ]
) }}

select
    row_number() over (order by dim_city.city_id, staging.timestamp) as weather_id,
    dim_city.city_id,
    staging.timestamp as measurement_timestamp,
    staging.temperature,
    staging.humidity,
    staging.wind_speed,
    staging.description,
    staging.pressure,
    staging.clouds,
    current_timestamp as created_at
from {{ ref('stg_weather_data') }} as staging
left join {{ ref('dim_city') }} as dim_city
    on staging.city = dim_city.city_name
where dim_city.city_id is not null
