

SELECT
    metadata.saved_at::TIMESTAMP as saved_at,
    metadata.category as load_category,
    metadata.identifier,
    metadata.timestamp::TIMESTAMP as load_timestamp,
    feature.properties.stationIdentifier as station_id,
    feature.id as station_url,
    feature.properties."@type" as station_type,
    feature.properties.stationIdentifier as station_identifier,
    feature.properties.name as station_name,
    feature.properties.timeZone as time_zone,
    feature.geometry.type as geometry_type,
    feature.geometry.coordinates[1] as longitude,
    feature.geometry.coordinates[2] as latitude,
    feature.properties.elevation.value as elevation_value,
    feature.properties.elevation.unitCode as elevation_unit,
    feature.properties.forecast as forecast_url,
    feature.properties.county as county_url,
    feature.properties.fireWeatherZone as fire_weather_zone_url
FROM read_json('../../datalake/raw/stations/*.json', auto_detect=true, union_by_name=true),
UNNEST(data.features) AS t(feature)