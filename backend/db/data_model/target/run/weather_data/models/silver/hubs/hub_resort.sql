
  
    
    

    create  table
      "weather"."main_silver"."hub_resort__dbt_tmp"
  
    as (
      

WITH source AS (
    SELECT DISTINCT
        identifier as resort_name
    FROM "weather"."main_bronze"."bronze_points"
)

SELECT
    MD5(resort_name) as resort_key,
    resort_name,
    CURRENT_TIMESTAMP as load_date,
    'resorts.yaml' as record_source
FROM source


    );
  
  
  