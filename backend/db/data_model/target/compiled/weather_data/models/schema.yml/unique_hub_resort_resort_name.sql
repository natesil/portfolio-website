
    
    

select
    resort_name as unique_field,
    count(*) as n_records

from "weather"."main_silver"."hub_resort"
where resort_name is not null
group by resort_name
having count(*) > 1


