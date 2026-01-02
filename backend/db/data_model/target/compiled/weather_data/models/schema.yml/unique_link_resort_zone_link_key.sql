
    
    

select
    link_key as unique_field,
    count(*) as n_records

from "weather"."main_silver"."link_resort_zone"
where link_key is not null
group by link_key
having count(*) > 1


