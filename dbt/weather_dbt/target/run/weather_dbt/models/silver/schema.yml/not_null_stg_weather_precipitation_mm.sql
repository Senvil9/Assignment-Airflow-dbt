
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select precipitation_mm
from "analytics"."silver"."stg_weather"
where precipitation_mm is null



  
  
      
    ) dbt_internal_test