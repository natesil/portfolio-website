{{
  config(
    materialized='incremental',
    unique_key='resort_key'
  )
}}

WITH source AS (
    SELECT DISTINCT
        identifier as resort_name
    FROM {{ ref('bronze_points') }}
)

SELECT
    MD5(resort_name) as resort_key,
    resort_name,
    CURRENT_TIMESTAMP as load_date,
    'resorts.yaml' as record_source
FROM source

{% if is_incremental() %}
  WHERE MD5(resort_name) NOT IN (SELECT resort_key FROM {{ this }})
{% endif %}
