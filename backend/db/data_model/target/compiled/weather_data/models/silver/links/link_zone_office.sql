

WITH source AS (
    SELECT DISTINCT
        REGEXP_EXTRACT(p.forecast_zone, '[^/]+$') as zone_id,
        p.grid_id as office_id
    FROM "weather"."main_bronze"."bronze_points" p
    WHERE p.forecast_zone IS NOT NULL
      AND p.grid_id IS NOT NULL
),

with_keys AS (
    SELECT
        MD5(zone_id) as zone_key,
        MD5(office_id) as office_key
    FROM source
)

SELECT
    MD5(zone_key || '|' || office_key) as link_key,
    zone_key,
    office_key,
    CURRENT_TIMESTAMP as load_date,
    'weather.gov' as record_source
FROM with_keys

