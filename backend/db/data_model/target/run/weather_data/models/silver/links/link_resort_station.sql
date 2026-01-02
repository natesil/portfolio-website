
  
    
    

    create  table
      "weather"."main_silver"."link_resort_station__dbt_tmp"
  
    as (
      

WITH source AS (
    SELECT DISTINCT
        o.identifier as resort_name,
        o.station_id,
        ROW_NUMBER() OVER (PARTITION BY o.identifier ORDER BY o.observation_time DESC) as station_rank
    FROM "weather"."main_bronze"."bronze_observations" o
    WHERE o.station_id IS NOT NULL
),

with_keys AS (
    SELECT
        MD5(resort_name) as resort_key,
        MD5(station_id) as station_key,
        station_rank
    FROM source
)

SELECT
    MD5(resort_key || '|' || station_key) as link_key,
    resort_key,
    station_key,
    station_rank,
    CURRENT_TIMESTAMP as load_date,
    'weather.gov' as record_source
FROM with_keys


    );
  
  
  