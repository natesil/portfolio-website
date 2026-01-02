
  
    
    

    create  table
      "weather"."main_silver"."sat_forecast_hourly__dbt_tmp"
  
    as (
      

WITH source AS (
    SELECT
        identifier,
        forecast_generated_at,
        forecast_updated_at,
        period_number,
        start_time,
        end_time,
        is_daytime,
        temperature,
        temperature_unit,
        wind_speed,
        wind_direction,
        icon_url,
        short_forecast,
        detailed_forecast,
        probability_of_precipitation_value,
        dewpoint_value,
        relative_humidity_value,
        load_timestamp
    FROM "weather"."main_bronze"."bronze_hourly_periods"
)

SELECT
    MD5(identifier) as resort_key,
    load_timestamp as load_date,
    'weather.gov' as record_source,
    forecast_generated_at,
    forecast_updated_at,
    period_number,
    start_time,
    end_time,
    is_daytime,
    temperature,
    temperature_unit,
    wind_speed,
    wind_direction,
    icon_url,
    short_forecast,
    detailed_forecast,
    probability_of_precipitation_value,
    dewpoint_value,
    relative_humidity_value
FROM source


    );
  
  
  