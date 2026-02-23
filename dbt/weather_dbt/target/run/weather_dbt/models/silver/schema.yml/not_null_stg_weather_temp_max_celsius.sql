
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select temp_max_celsius
from "analytics"."silver"."stg_weather"
where temp_max_celsius is null



  
  
      
    ) dbt_internal_test