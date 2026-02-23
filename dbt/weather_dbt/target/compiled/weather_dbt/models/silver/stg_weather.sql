

WITH ranked_weather AS (
    SELECT
        *,  -- Keep raw types, cast later
        ROW_NUMBER() OVER(
            PARTITION BY city_name, day 
            ORDER BY ingested_at DESC
        ) AS rn
    FROM "analytics"."bronze"."weather_raw"
),  -- ✅ Fixed: Added comma

deduped_weather AS (
    SELECT
        MD5(city_name || '|' || day::TEXT) AS weather_surrogate_key,  -- ✅ PostgreSQL fix
        
        CAST(day AS DATE) AS observation_date,
        CAST(city_name AS VARCHAR(50)) AS city_name,
        CAST(latitude AS DECIMAL(9,6)) AS latitude,
        CAST(longitude AS DECIMAL(9,6)) AS longitude,
        CAST(temp_max AS DECIMAL(5,2)) AS temp_max_celsius,
        CAST(temp_min AS DECIMAL(5,2)) AS temp_min_celsius,
        CAST(precipitation_sum AS DECIMAL(6,2)) AS precipitation_mm,
        CAST(ingested_at AS TIMESTAMP) AS ingested_at
    FROM ranked_weather
    WHERE rn = 1
        AND city_name IS NOT NULL 
        AND latitude IS NOT NULL 
        AND longitude IS NOT NULL
        AND day IS NOT NULL
        AND temp_max IS NOT NULL 
        AND temp_min IS NOT NULL
)

SELECT * FROM deduped_weather