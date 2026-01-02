
  
    
    

    create  table
      "weather"."main_silver"."link_resort_zone__dbt_tmp"
  
    as (
      

WITH source AS (
    SELECT DISTINCT
        p.identifier as resort_name,
        REGEXP_EXTRACT(p.forecast_zone, '[^/]+$') as zone_id
    FROM "weather"."main_bronze"."bronze_points" p
    WHERE p.forecast_zone IS NOT NULL
),

with_keys AS (
    SELECT
        MD5(resort_name) as resort_key,
        MD5(zone_id) as zone_key
    FROM source
)

SELECT
    MD5(resort_key || '|' || zone_key) as link_key,
    resort_key,
    zone_key,
    CURRENT_TIMESTAMP as load_date,
    'weather.gov' as record_source
FROM with_keys


    );
  
  
  