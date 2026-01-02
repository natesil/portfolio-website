
    
    

with child as (
    select station_key as from_field
    from "weather"."main_silver"."link_resort_station"
    where station_key is not null
),

parent as (
    select station_key as to_field
    from "weather"."main_silver"."hub_station"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


