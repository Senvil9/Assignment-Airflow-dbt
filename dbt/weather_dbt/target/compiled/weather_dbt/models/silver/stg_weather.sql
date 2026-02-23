

WITH ranked AS (
    SELECT
        day:: DATE AS day,
        latitude:: NUMERIC AS latitude,
        longitude:: NUMERIC AS longitude,
        temp_max:: NUMERIC AS temp_max,
        temp_min:: NUMERIC AS temp_min,
        precipitation_sum:: NUMERIC AS precipitation_sum,
        ingested_at:: TIMESTAMP AS ingested_at,
        ROW_NUMBER() OVER(PARTITION BY day ORDER BY ingested_at DESC) AS rn
    FROM "analytics"."bronze"."weather_raw"
)

SELECT
    day,
    latitude,
    longitude,
    temp_max,
    temp_min,
    precipitation_sum,
    ingested_at
FROM ranked
WHERE rn = 1