
    
    

select
    office_key as unique_field,
    count(*) as n_records

from "weather"."main_silver"."hub_office"
where office_key is not null
group by office_key
having count(*) > 1


