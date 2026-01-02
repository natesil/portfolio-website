{{
  config(
    materialized='incremental',
    unique_key='link_key'
  )
}}

WITH source AS (
    SELECT DISTINCT
        REGEXP_EXTRACT(p.forecast_zone, '[^/]+$') as zone_id,
        p.grid_id as office_id
    FROM {{ ref('bronze_points') }} p
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

{% if is_incremental() %}
  WHERE MD5(zone_key || '|' || office_key) NOT IN (SELECT link_key FROM {{ this }})
{% endif %}
