
    
    

with child as (
    select resort_key as from_field
    from "weather"."main_silver"."sat_forecast_hourly"
    where resort_key is not null
),

parent as (
    select resort_key as to_field
    from "weather"."main_silver"."hub_resort"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


