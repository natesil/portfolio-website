
    
    

select
    zone_id as unique_field,
    count(*) as n_records

from "weather"."main_silver"."hub_zone"
where zone_id is not null
group by zone_id
having count(*) > 1


