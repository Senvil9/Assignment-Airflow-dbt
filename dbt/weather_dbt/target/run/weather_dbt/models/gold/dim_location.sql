
  
    

  create  table "analytics"."gold"."dim_location__dbt_tmp"
  
  
    as
  
  (
    

SELECT DISTINCT
    latitude,
    longitude
FROM "analytics"."silver"."stg_weather"
  );
  