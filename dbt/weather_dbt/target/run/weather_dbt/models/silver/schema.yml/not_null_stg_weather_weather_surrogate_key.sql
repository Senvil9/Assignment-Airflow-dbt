
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select weather_surrogate_key
from "analytics"."silver"."stg_weather"
where weather_surrogate_key is null



  
  
      
    ) dbt_internal_test