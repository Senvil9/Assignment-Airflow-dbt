{{ congif(materialized= 'table') }}

WITH bounds AS (
    SELECT
        MIN(day) AS min_day,
        MAX(day) AS max_day
    FROM {{ ref('stg_weather') }}
),
dates AS (
    SELECT
        GENERATE_SERIES(min_day, max_day, INTERVAL '1 day'):: DATE AS date_day
    FROM bounds
)
SELECT
    date_day,
    EXTRACT(year FROM date_day):: INT AS year,
    EXTRACT(month FROM date_day):: INT AS month,
    TO_CHAR(date_day, 'Mon') AS month_name,
    EXTRACT(day FROM date_day) AS day_of_month,
    EXTRACT(dow FROM date_day):: INT AS day_of_week,
    