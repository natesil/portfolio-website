
    
    

with child as (
    select office_key as from_field
    from "weather"."main_silver"."link_zone_office"
    where office_key is not null
),

parent as (
    select office_key as to_field
    from "weather"."main_silver"."hub_office"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


