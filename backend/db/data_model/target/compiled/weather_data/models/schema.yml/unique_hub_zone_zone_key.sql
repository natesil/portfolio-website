
    
    

select
    zone_key as unique_field,
    count(*) as n_records

from "weather"."main_silver"."hub_zone"
where zone_key is not null
group by zone_key
having count(*) > 1


