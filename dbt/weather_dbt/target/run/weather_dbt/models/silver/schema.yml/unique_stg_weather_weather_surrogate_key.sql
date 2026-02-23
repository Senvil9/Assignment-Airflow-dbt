
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    weather_surrogate_key as unique_field,
    count(*) as n_records

from "analytics"."silver"."stg_weather"
where weather_surrogate_key is not null
group by weather_surrogate_key
having count(*) > 1



  
  
      
    ) dbt_internal_test