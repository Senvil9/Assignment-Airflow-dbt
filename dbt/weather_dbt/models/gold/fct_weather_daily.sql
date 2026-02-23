{{ config(materialized= 'table') }}

SELECT
    day AS date_day,
    latitude,
    longitude,
    temp_max,
    temp_min,
    temp_max - temp_min AS temp_range,
    precipitation_sum
FROM {{ ref('stg_weather')}}