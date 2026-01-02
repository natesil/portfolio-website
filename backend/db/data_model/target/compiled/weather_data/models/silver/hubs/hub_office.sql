

WITH source AS (
    SELECT DISTINCT
        grid_id as office_id
    FROM "weather"."main_bronze"."bronze_points"
    WHERE grid_id IS NOT NULL
)

SELECT
    MD5(office_id) as office_key,
    office_id,
    CURRENT_TIMESTAMP as load_date,
    'weather.gov' as record_source
FROM source

