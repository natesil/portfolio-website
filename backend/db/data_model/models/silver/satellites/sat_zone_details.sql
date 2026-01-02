{{
  config(
    materialized='incremental',
    unique_key=['zone_key', 'load_date']
  )
}}

WITH source AS (
    SELECT
        zone_id_code as zone_id,
        zone_name as name,
        state,
        effective_date,
        expiration_date,
        load_timestamp
    FROM {{ ref('bronze_zones') }}
    WHERE zone_id_code IS NOT NULL
)

SELECT
    MD5(zone_id) as zone_key,
    load_timestamp as load_date,
    NULL as load_end_date,
    'weather.gov' as record_source,
    name,
    state,
    effective_date,
    expiration_date,
    NULL as time_zone
FROM source

{% if is_incremental() %}
  WHERE (MD5(zone_id), load_timestamp) NOT IN (
    SELECT zone_key, load_date FROM {{ this }}
  )
{% endif %}
