{{
  config(
    materialized='incremental',
    unique_key=['station_key', 'load_date']
  )
}}

WITH source AS (
    SELECT
        station_identifier as station_id,
        station_name as name,
        latitude,
        longitude,
        elevation_value,
        elevation_unit,
        time_zone,
        load_timestamp
    FROM {{ ref('bronze_stations') }}
    WHERE station_identifier IS NOT NULL
)

SELECT
    MD5(station_id) as station_key,
    load_timestamp as load_date,
    NULL as load_end_date,
    'weather.gov' as record_source,
    name,
    latitude,
    longitude,
    elevation_value,
    elevation_unit,
    time_zone
FROM source

{% if is_incremental() %}
  WHERE (MD5(station_id), load_timestamp) NOT IN (
    SELECT station_key, load_date FROM {{ this }}
  )
{% endif %}
