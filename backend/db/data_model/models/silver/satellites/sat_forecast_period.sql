{{
  config(
    materialized='incremental',
    unique_key=['resort_key', 'forecast_generated_at', 'period_number']
  )
}}

WITH source AS (
    SELECT
        identifier,
        forecast_generated_at,
        forecast_updated_at,
        period_number,
        period_name,
        start_time,
        end_time,
        is_daytime,
        temperature,
        temperature_unit,
        temperature_trend,
        wind_speed,
        wind_direction,
        icon_url,
        short_forecast,
        detailed_forecast,
        probability_of_precipitation_value,
        dewpoint_value,
        relative_humidity_value,
        load_timestamp
    FROM {{ ref('bronze_forecast_periods') }}
)

SELECT
    MD5(identifier) as resort_key,
    load_timestamp as load_date,
    'weather.gov' as record_source,
    forecast_generated_at,
    forecast_updated_at,
    period_number,
    period_name,
    start_time,
    end_time,
    is_daytime,
    temperature,
    temperature_unit,
    temperature_trend,
    wind_speed,
    wind_direction,
    icon_url,
    short_forecast,
    detailed_forecast,
    probability_of_precipitation_value,
    dewpoint_value,
    relative_humidity_value
FROM source

{% if is_incremental() %}
  WHERE (MD5(identifier), forecast_generated_at, period_number) NOT IN (
    SELECT resort_key, forecast_generated_at, period_number FROM {{ this }}
  )
{% endif %}
