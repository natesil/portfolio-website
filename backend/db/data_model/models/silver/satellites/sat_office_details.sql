{{
  config(
    materialized='incremental',
    unique_key=['office_key', 'load_date']
  )
}}

WITH source AS (
    SELECT DISTINCT
        grid_id as office_id,
        forecast_office as office_url,
        load_timestamp
    FROM {{ ref('bronze_points') }}
    WHERE grid_id IS NOT NULL
)

SELECT
    MD5(office_id) as office_key,
    load_timestamp as load_date,
    NULL as load_end_date,
    'weather.gov' as record_source,
    NULL as name,
    office_url as url
FROM source

{% if is_incremental() %}
  WHERE (MD5(office_id), load_timestamp) NOT IN (
    SELECT office_key, load_date FROM {{ this }}
  )
{% endif %}
