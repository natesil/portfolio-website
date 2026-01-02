
    
    

select
    station_key as unique_field,
    count(*) as n_records

from "weather"."main_silver"."hub_station"
where station_key is not null
group by station_key
having count(*) > 1


