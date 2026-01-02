
  
    
    

    create  table
      "weather"."main_silver"."hub_station__dbt_tmp"
  
    as (
      

WITH source AS (
    SELECT DISTINCT
        station_identifier as station_id
    FROM "weather"."main_bronze"."bronze_stations"
    WHERE station_identifier IS NOT NULL
)

SELECT
    MD5(station_id) as station_key,
    station_id,
    CURRENT_TIMESTAMP as load_date,
    'weather.gov' as record_source
FROM source


    );
  
  
  