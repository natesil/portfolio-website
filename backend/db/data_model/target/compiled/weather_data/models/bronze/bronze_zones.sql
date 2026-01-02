

SELECT
    metadata.saved_at::TIMESTAMP as saved_at,
    metadata.category as load_category,
    metadata.identifier,
    metadata.timestamp::TIMESTAMP as load_timestamp,
    data.properties."@id" as zone_id,
    data.properties."@type" as zone_type,
    data.properties.id as zone_id_code,
    data.properties.type as zone_type_code,
    data.properties.name as zone_name,
    data.properties.state,
    data.properties.effectiveDate::TIMESTAMP as effective_date,
    data.properties.expirationDate::TIMESTAMP as expiration_date,
    data.properties.cwa,
    data.properties.forecastOffices as forecast_offices,
    data.properties.timeZone as time_zones,
    data.properties.observationStations as observation_stations,
    data.properties.radarStation as radar_station,
    data.geometry.type as geometry_type,
    data.geometry.coordinates as geometry_coordinates
FROM read_json('../../datalake/raw/zones/*.json', auto_detect=true, union_by_name=true)