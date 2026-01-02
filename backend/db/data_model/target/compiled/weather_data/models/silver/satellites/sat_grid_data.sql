

WITH source AS (
    SELECT
        identifier,
        update_time,
        valid_times,
        temperature,
        dewpoint,
        max_temperature,
        min_temperature,
        relative_humidity,
        apparent_temperature,
        heat_index,
        wind_chill,
        sky_cover,
        wind_direction,
        wind_speed,
        wind_gust,
        probability_of_precipitation,
        quantitative_precipitation,
        snowfall_amount,
        ice_accumulation,
        weather,
        hazards,
        load_timestamp
    FROM "weather"."main_bronze"."bronze_grid_data"
)

SELECT
    MD5(identifier) as resort_key,
    load_timestamp as load_date,
    'weather.gov' as record_source,
    update_time,
    valid_times,
    temperature,
    dewpoint,
    max_temperature,
    min_temperature,
    relative_humidity,
    apparent_temperature,
    heat_index,
    wind_chill,
    sky_cover,
    wind_direction,
    wind_speed,
    wind_gust,
    probability_of_precipitation,
    quantitative_precipitation,
    snowfall_amount,
    ice_accumulation,
    weather,
    hazards
FROM source

