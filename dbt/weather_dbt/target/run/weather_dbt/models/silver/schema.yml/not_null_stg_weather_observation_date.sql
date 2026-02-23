
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select observation_date
from "analytics"."silver"."stg_weather"
where observation_date is null



  
  
      
    ) dbt_internal_test