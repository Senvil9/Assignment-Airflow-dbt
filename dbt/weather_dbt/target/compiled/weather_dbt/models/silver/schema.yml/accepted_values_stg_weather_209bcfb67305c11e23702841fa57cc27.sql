
    
    

with all_values as (

    select
        city_name as value_field,
        count(*) as n_records

    from "analytics"."silver"."stg_weather"
    group by city_name

)

select *
from all_values
where value_field not in (
    'Kathmandu','Pokhara','Biratnagar','Birgunj'
)


