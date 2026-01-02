
    
    

select
    resort_key as unique_field,
    count(*) as n_records

from "weather"."main_silver"."hub_resort"
where resort_key is not null
group by resort_key
having count(*) > 1


