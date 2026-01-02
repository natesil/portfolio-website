

WITH source AS (
    SELECT
        identifier as resort_name,
        state,
        latitude,
        longitude,
        grid_id,
        grid_x,
        grid_y,
        load_timestamp
    FROM "weather"."main_bronze"."bronze_points"
),

with_keys AS (
    SELECT
        MD5(resort_name) as resort_key,
        state,
        latitude,
        longitude,
        grid_id,
        grid_x,
        grid_y,
        load_timestamp as load_date
    FROM source
)

SELECT
    resort_key,
    load_date,
    NULL as load_end_date,
    'weather.gov' as record_source,
    state,
    latitude,
    longitude,
    NULL as full_name,
    NULL as region,
    grid_id,
    grid_x,
    grid_y
FROM with_keys

