
  
    
    

    create  table
      "weather"."main_silver"."hub_zone__dbt_tmp"
  
    as (
      

WITH source AS (
    SELECT DISTINCT
        zone_id_code as zone_id
    FROM "weather"."main_bronze"."bronze_zones"
    WHERE zone_id_code IS NOT NULL
)

SELECT
    MD5(zone_id) as zone_key,
    zone_id,
    CURRENT_TIMESTAMP as load_date,
    'weather.gov' as record_source
FROM source


    );
  
  
  