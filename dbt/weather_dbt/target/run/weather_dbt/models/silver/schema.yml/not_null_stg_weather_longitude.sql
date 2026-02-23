
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select longitude
from "analytics"."silver"."stg_weather"
where longitude is null



  
  
      
    ) dbt_internal_test