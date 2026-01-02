{{
  config(
    materialized='incremental',
    unique_key=['station_key', 'observation_time']
  )
}}

WITH source AS (
    SELECT
        station_id,
        observation_time,
        text_description,
        temperature_value,
        temperature_unit,
        dewpoint_value,
        dewpoint_unit,
        wind_direction_value,
        wind_speed_value,
        wind_speed_unit,
        wind_gust_value,
        barometric_pressure_value,
        sea_level_pressure_value,
        visibility_value,
        relative_humidity_value,
        wind_chill_value,
        heat_index_value,
        precipitation_last_hour_value,
        precipitation_last_3hours_value,
        precipitation_last_6hours_value,
        load_timestamp
    FROM {{ ref('bronze_observations') }}
    WHERE station_id IS NOT NULL
)

SELECT
    MD5(station_id) as station_key,
    load_timestamp as load_date,
    'weather.gov' as record_source,
    observation_time,
    text_description,
    temperature_value,
    temperature_unit,
    dewpoint_value,
    dewpoint_unit,
    wind_direction_value,
    wind_speed_value,
    wind_speed_unit,
    wind_gust_value,
    barometric_pressure_value,
    sea_level_pressure_value,
    visibility_value,
    relative_humidity_value,
    wind_chill_value,
    heat_index_value,
    precipitation_last_hour_value,
    precipitation_last_3hours_value,
    precipitation_last_6hours_value
FROM source

{% if is_incremental() %}
  WHERE (MD5(station_id), observation_time) NOT IN (
    SELECT station_key, observation_time FROM {{ this }}
  )
{% endif %}
