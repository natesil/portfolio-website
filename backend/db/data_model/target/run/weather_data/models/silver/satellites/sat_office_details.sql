
  
    
    

    create  table
      "weather"."main_silver"."sat_office_details__dbt_tmp"
  
    as (
      

WITH source AS (
    SELECT DISTINCT
        grid_id as office_id,
        forecast_office as office_url,
        load_timestamp
    FROM "weather"."main_bronze"."bronze_points"
    WHERE grid_id IS NOT NULL
)

SELECT
    MD5(office_id) as office_key,
    load_timestamp as load_date,
    NULL as load_end_date,
    'weather.gov' as record_source,
    NULL as name,
    office_url as url
FROM source


    );
  
  
  