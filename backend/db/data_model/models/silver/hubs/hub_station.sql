{{
  config(
    materialized='incremental',
    unique_key='station_key'
  )
}}

WITH source AS (
    SELECT DISTINCT
        station_identifier as station_id
    FROM {{ ref('bronze_stations') }}
    WHERE station_identifier IS NOT NULL
)

SELECT
    MD5(station_id) as station_key,
    station_id,
    CURRENT_TIMESTAMP as load_date,
    'weather.gov' as record_source
FROM source

{% if is_incremental() %}
  WHERE MD5(station_id) NOT IN (SELECT station_key FROM {{ this }})
{% endif %}
