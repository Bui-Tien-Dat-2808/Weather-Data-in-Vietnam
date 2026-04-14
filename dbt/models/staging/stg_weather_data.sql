-- Staging model: Clean and prepare raw weather data
-- Converts raw timestamp and standardizes data types
{{ config(
    materialized='view',
    tags=['staging', 'daily']
) }}

select
    city,
    timestamp,
    temperature,
    humidity,
    wind_speed,
    description,
    pressure,
    clouds,
    created_at
from {{ source('weather_analytics', 'weather_data') }}
where temperature is not null
  and humidity is not null
  and wind_speed is not null
order by city, timestamp
