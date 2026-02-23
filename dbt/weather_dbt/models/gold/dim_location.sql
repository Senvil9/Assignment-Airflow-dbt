{{ config(materialized= 'table') }}

SELECT DISTINCT
    latitude,
    longitude
FROM {{ ref('stg_weather') }}