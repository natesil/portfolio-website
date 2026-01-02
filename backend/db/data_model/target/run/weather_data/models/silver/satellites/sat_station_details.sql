
  
    
    

    create  table
      "weather"."main_silver"."sat_station_details__dbt_tmp"
  
    as (
      

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
    FROM "weather"."main_bronze"."bronze_stations"
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


    );
  
  
  