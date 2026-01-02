
    
    

select
    office_id as unique_field,
    count(*) as n_records

from "weather"."main_silver"."hub_office"
where office_id is not null
group by office_id
having count(*) > 1


