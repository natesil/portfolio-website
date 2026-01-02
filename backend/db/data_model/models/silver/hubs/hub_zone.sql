{{
  config(
    materialized='incremental',
    unique_key='zone_key'
  )
}}

WITH source AS (
    SELECT DISTINCT
        zone_id_code as zone_id
    FROM {{ ref('bronze_zones') }}
    WHERE zone_id_code IS NOT NULL
)

SELECT
    MD5(zone_id) as zone_key,
    zone_id,
    CURRENT_TIMESTAMP as load_date,
    'weather.gov' as record_source
FROM source

{% if is_incremental() %}
  WHERE MD5(zone_id) NOT IN (SELECT zone_key FROM {{ this }})
{% endif %}
