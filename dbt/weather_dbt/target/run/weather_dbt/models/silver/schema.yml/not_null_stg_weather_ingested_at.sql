
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ingested_at
from "analytics"."silver"."stg_weather"
where ingested_at is null



  
  
      
    ) dbt_internal_test