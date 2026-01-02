
  
    
    

    create  table
      "weather"."main_bronze"."bronze_grid_data__dbt_tmp"
  
    as (
      

SELECT
    metadata.saved_at::TIMESTAMP as saved_at,
    metadata.category as load_category,
    metadata.identifier,
    metadata.timestamp::TIMESTAMP as load_timestamp,
    data.properties.gridId as grid_id,
    data.properties.gridX as grid_x,
    data.properties.gridY as grid_y,
    data.properties.forecastOffice as forecast_office,
    data.properties.updateTime::TIMESTAMP as update_time,
    data.properties.validTimes as valid_times,
    data.properties.elevation.value as elevation_value,
    data.properties.elevation.unitCode as elevation_unit,
    data.geometry.type as geometry_type,
    data.geometry.coordinates as geometry_coordinates,
    -- Store weather data layers as JSON for flexibility
    data.properties.temperature,
    data.properties.dewpoint,
    data.properties.maxTemperature as max_temperature,
    data.properties.minTemperature as min_temperature,
    data.properties.relativeHumidity as relative_humidity,
    data.properties.apparentTemperature as apparent_temperature,
    data.properties.heatIndex as heat_index,
    data.properties.windChill as wind_chill,
    data.properties.skyCover as sky_cover,
    data.properties.windDirection as wind_direction,
    data.properties.windSpeed as wind_speed,
    data.properties.windGust as wind_gust,
    data.properties.probabilityOfPrecipitation as probability_of_precipitation,
    data.properties.quantitativePrecipitation as quantitative_precipitation,
    data.properties.iceAccumulation as ice_accumulation,
    data.properties.snowfallAmount as snowfall_amount,
    data.properties.snowLevel as snow_level,
    data.properties.ceilingHeight as ceiling_height,
    data.properties.visibility,
    data.properties.weather,
    data.properties.hazards
FROM read_json('../../datalake/raw/grid_data/*.json', auto_detect=true, union_by_name=true)
    );
  
  