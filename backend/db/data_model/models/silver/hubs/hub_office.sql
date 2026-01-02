{{
  config(
    materialized='incremental',
    unique_key='office_key'
  )
}}

WITH source AS (
    SELECT DISTINCT
        grid_id as office_id
    FROM {{ ref('bronze_points') }}
    WHERE grid_id IS NOT NULL
)

SELECT
    MD5(office_id) as office_key,
    office_id,
    CURRENT_TIMESTAMP as load_date,
    'weather.gov' as record_source
FROM source

{% if is_incremental() %}
  WHERE MD5(office_id) NOT IN (SELECT office_key FROM {{ this }})
{% endif %}
