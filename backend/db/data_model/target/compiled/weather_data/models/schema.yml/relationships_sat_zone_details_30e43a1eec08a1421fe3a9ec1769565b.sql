
    
    

with child as (
    select zone_key as from_field
    from "weather"."main_silver"."sat_zone_details"
    where zone_key is not null
),

parent as (
    select zone_key as to_field
    from "weather"."main_silver"."hub_zone"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


