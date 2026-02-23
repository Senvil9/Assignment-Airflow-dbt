
  
    

  create  table "analytics"."gold"."fct_weather_daily__dbt_tmp"
  
  
    as
  
  (
    

WITH daily_metrics AS (
    SELECT 
        weather_surrogate_key,
        city_name,
        observation_date,
        latitude,
        longitude,
        
        -- Basic metrics
        (temp_max_celsius + temp_min_celsius) / 2 AS avg_temp,
        temp_max_celsius AS max_temp,
        temp_min_celsius AS min_temp,
        precipitation_mm AS total_precipitation,
        
        -- 7-day moving average temperature
        AVG((temp_max_celsius + temp_min_celsius) / 2) 
            OVER (
                PARTITION BY city_name 
                ORDER BY observation_date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS moving_avg_7d_temp

    FROM "analytics"."silver"."stg_weather"
),

final_summary AS (
    SELECT 
        weather_surrogate_key,
        city_name,
        observation_date,
        latitude,
        longitude,
        ROUND(avg_temp, 2) AS avg_temp,
        ROUND(max_temp, 2) AS max_temp,
        ROUND(min_temp, 2) AS min_temp,
        ROUND(total_precipitation, 2) AS total_precipitation,
        ROUND(moving_avg_7d_temp, 2) AS moving_avg_7d_temp

    FROM daily_metrics
)

SELECT * FROM final_summary
  );
  