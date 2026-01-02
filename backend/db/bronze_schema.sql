-- ============================================================================
-- Bronze Layer Schema
-- Database: DuckDB
-- ============================================================================
-- Purpose: Flattened tables loaded from Raw JSON (datalake/raw/)
-- Bronze tables flatten nested structures and unpack arrays for easier querying
-- before transformation into Data Vault (Silver layer)
-- ============================================================================

-- Points: Grid metadata for each resort location
CREATE TABLE IF NOT EXISTS bronze_points (
    -- Metadata from wrapper
    saved_at TIMESTAMP NOT NULL,
    load_category VARCHAR(50) NOT NULL,
    identifier VARCHAR(100) NOT NULL,
    load_timestamp TIMESTAMP NOT NULL,

    -- Points data (from properties)
    point_id VARCHAR(500) NOT NULL,
    point_type VARCHAR(50),
    cwa VARCHAR(10),
    forecast_office VARCHAR(500),
    grid_id VARCHAR(10),
    grid_x INTEGER,
    grid_y INTEGER,

    -- URLs
    forecast_url VARCHAR(500),
    forecast_hourly_url VARCHAR(500),
    forecast_grid_data_url VARCHAR(500),
    observation_stations_url VARCHAR(500),
    forecast_zone VARCHAR(500),
    county_url VARCHAR(500),
    fire_weather_zone VARCHAR(500),

    -- Location
    time_zone VARCHAR(50),
    radar_station VARCHAR(10),
    city VARCHAR(100),
    state VARCHAR(2),

    -- Geometry
    geometry_type VARCHAR(20),
    longitude DOUBLE,
    latitude DOUBLE,
    elevation DOUBLE,

    PRIMARY KEY (identifier, load_timestamp)
);

-- Forecast Periods: 12-hour forecast periods (unpacked from periods array)
CREATE TABLE IF NOT EXISTS bronze_forecast_periods (
    -- Metadata from wrapper
    saved_at TIMESTAMP NOT NULL,
    load_category VARCHAR(50) NOT NULL,
    identifier VARCHAR(100) NOT NULL,
    load_timestamp TIMESTAMP NOT NULL,

    -- Forecast metadata
    forecast_generated_at TIMESTAMP,
    forecast_updated_at TIMESTAMP,
    forecast_units VARCHAR(20),
    forecast_generator VARCHAR(100),
    elevation_value DOUBLE,
    elevation_unit VARCHAR(50),

    -- Period identification
    period_number INTEGER NOT NULL,
    period_name VARCHAR(50),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    is_daytime BOOLEAN,

    -- Temperature
    temperature INTEGER,
    temperature_unit VARCHAR(1),
    temperature_trend VARCHAR(20),

    -- Wind
    wind_speed VARCHAR(50),
    wind_direction VARCHAR(10),

    -- Forecast text
    icon_url VARCHAR(500),
    short_forecast TEXT,
    detailed_forecast TEXT,

    -- Probabilities and measurements (flattened QuantitativeValue)
    probability_of_precipitation_value DOUBLE,
    probability_of_precipitation_unit VARCHAR(50),
    dewpoint_value DOUBLE,
    dewpoint_unit VARCHAR(50),
    relative_humidity_value DOUBLE,
    relative_humidity_unit VARCHAR(50),

    PRIMARY KEY (identifier, load_timestamp, period_number)
);

-- Hourly Forecast Periods: Hourly forecast periods (unpacked from periods array)
CREATE TABLE IF NOT EXISTS bronze_hourly_periods (
    -- Metadata from wrapper
    saved_at TIMESTAMP NOT NULL,
    load_category VARCHAR(50) NOT NULL,
    identifier VARCHAR(100) NOT NULL,
    load_timestamp TIMESTAMP NOT NULL,

    -- Forecast metadata
    forecast_generated_at TIMESTAMP,
    forecast_updated_at TIMESTAMP,
    forecast_units VARCHAR(20),
    forecast_generator VARCHAR(100),
    elevation_value DOUBLE,
    elevation_unit VARCHAR(50),

    -- Period identification
    period_number INTEGER NOT NULL,
    period_name VARCHAR(50),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    is_daytime BOOLEAN,

    -- Temperature
    temperature INTEGER,
    temperature_unit VARCHAR(1),
    temperature_trend VARCHAR(20),

    -- Wind
    wind_speed VARCHAR(50),
    wind_direction VARCHAR(10),

    -- Forecast text
    icon_url VARCHAR(500),
    short_forecast TEXT,
    detailed_forecast TEXT,

    -- Probabilities and measurements (flattened QuantitativeValue)
    probability_of_precipitation_value DOUBLE,
    probability_of_precipitation_unit VARCHAR(50),
    dewpoint_value DOUBLE,
    dewpoint_unit VARCHAR(50),
    relative_humidity_value DOUBLE,
    relative_humidity_unit VARCHAR(50),

    PRIMARY KEY (identifier, load_timestamp, period_number)
);

-- Observations: Station observations (flattened)
CREATE TABLE IF NOT EXISTS bronze_observations (
    -- Metadata from wrapper
    saved_at TIMESTAMP NOT NULL,
    load_category VARCHAR(50) NOT NULL,
    identifier VARCHAR(100) NOT NULL,
    load_timestamp TIMESTAMP NOT NULL,

    -- Observation metadata
    observation_id VARCHAR(500),
    observation_type VARCHAR(50),
    station_url VARCHAR(500),
    station_id VARCHAR(20),
    observation_time TIMESTAMP NOT NULL,
    raw_message TEXT,
    text_description VARCHAR(500),
    icon_url VARCHAR(500),

    -- Geometry
    geometry_type VARCHAR(20),
    longitude DOUBLE,
    latitude DOUBLE,
    elevation_value DOUBLE,
    elevation_unit VARCHAR(50),

    -- Temperature measurements (flattened QuantitativeValue)
    temperature_value DOUBLE,
    temperature_unit VARCHAR(50),
    dewpoint_value DOUBLE,
    dewpoint_unit VARCHAR(50),
    heat_index_value DOUBLE,
    heat_index_unit VARCHAR(50),
    wind_chill_value DOUBLE,
    wind_chill_unit VARCHAR(50),
    max_temperature_last_24h_value DOUBLE,
    max_temperature_last_24h_unit VARCHAR(50),
    min_temperature_last_24h_value DOUBLE,
    min_temperature_last_24h_unit VARCHAR(50),

    -- Wind measurements
    wind_direction_value DOUBLE,
    wind_direction_unit VARCHAR(50),
    wind_speed_value DOUBLE,
    wind_speed_unit VARCHAR(50),
    wind_gust_value DOUBLE,
    wind_gust_unit VARCHAR(50),

    -- Pressure measurements
    barometric_pressure_value DOUBLE,
    barometric_pressure_unit VARCHAR(50),
    sea_level_pressure_value DOUBLE,
    sea_level_pressure_unit VARCHAR(50),

    -- Precipitation measurements
    precipitation_last_hour_value DOUBLE,
    precipitation_last_hour_unit VARCHAR(50),
    precipitation_last_3hours_value DOUBLE,
    precipitation_last_3hours_unit VARCHAR(50),
    precipitation_last_6hours_value DOUBLE,
    precipitation_last_6hours_unit VARCHAR(50),

    -- Other measurements
    visibility_value DOUBLE,
    visibility_unit VARCHAR(50),
    relative_humidity_value DOUBLE,
    relative_humidity_unit VARCHAR(50),

    PRIMARY KEY (identifier, observation_time, load_timestamp)
);

-- Stations: Station metadata (from features array)
CREATE TABLE IF NOT EXISTS bronze_stations (
    -- Metadata from wrapper
    saved_at TIMESTAMP NOT NULL,
    load_category VARCHAR(50) NOT NULL,
    identifier VARCHAR(100) NOT NULL,
    load_timestamp TIMESTAMP NOT NULL,

    -- Station data
    station_id VARCHAR(500) NOT NULL,
    station_type VARCHAR(50),
    station_identifier VARCHAR(20) NOT NULL,
    station_name VARCHAR(200),
    time_zone VARCHAR(50),

    -- Location
    geometry_type VARCHAR(20),
    longitude DOUBLE,
    latitude DOUBLE,
    elevation_value DOUBLE,
    elevation_unit VARCHAR(50),

    -- URLs
    forecast_url VARCHAR(500),
    county_url VARCHAR(500),
    fire_weather_zone_url VARCHAR(500),

    PRIMARY KEY (identifier, station_identifier, load_timestamp)
);

-- Zones: Forecast zone metadata (from features array)
CREATE TABLE IF NOT EXISTS bronze_zones (
    -- Metadata from wrapper
    saved_at TIMESTAMP NOT NULL,
    load_category VARCHAR(50) NOT NULL,
    identifier VARCHAR(100) NOT NULL,
    load_timestamp TIMESTAMP NOT NULL,

    -- Zone data
    zone_id VARCHAR(500) NOT NULL,
    zone_type VARCHAR(50),
    zone_id_code VARCHAR(20) NOT NULL,
    zone_type_code VARCHAR(50),
    zone_name VARCHAR(200),
    state VARCHAR(2),
    effective_date TIMESTAMP,
    expiration_date TIMESTAMP,

    -- Arrays stored as JSON (can be unnested later if needed)
    cwa JSON,
    forecast_offices JSON,
    time_zones JSON,
    observation_stations JSON,
    radar_station VARCHAR(10),

    -- Geometry (polygon)
    geometry_type VARCHAR(20),
    geometry_coordinates JSON,

    PRIMARY KEY (identifier, zone_id_code, load_timestamp)
);

-- Grid Data: Raw numerical forecast data
-- Note: This is a complex structure with many optional time-series layers
-- Storing the data layers as JSON for flexibility
CREATE TABLE IF NOT EXISTS bronze_grid_data (
    -- Metadata from wrapper
    saved_at TIMESTAMP NOT NULL,
    load_category VARCHAR(50) NOT NULL,
    identifier VARCHAR(100) NOT NULL,
    load_timestamp TIMESTAMP NOT NULL,

    -- Grid metadata
    grid_id VARCHAR(10),
    grid_x INTEGER,
    grid_y INTEGER,
    forecast_office VARCHAR(500),
    update_time TIMESTAMP,
    valid_times VARCHAR(500),
    elevation_value DOUBLE,
    elevation_unit VARCHAR(50),

    -- Geometry
    geometry_type VARCHAR(20),
    geometry_coordinates JSON,

    -- Weather data layers (stored as JSON for flexibility)
    -- Each layer has format: {uom: "unit", values: [{validTime: "...", value: ...}]}
    temperature JSON,
    dewpoint JSON,
    max_temperature JSON,
    min_temperature JSON,
    relative_humidity JSON,
    apparent_temperature JSON,
    heat_index JSON,
    wind_chill JSON,
    sky_cover JSON,
    wind_direction JSON,
    wind_speed JSON,
    wind_gust JSON,
    probability_of_precipitation JSON,
    quantitative_precipitation JSON,
    ice_accumulation JSON,
    snowfall_amount JSON,
    snow_level JSON,
    ceiling_height JSON,
    visibility JSON,
    weather JSON,
    hazards JSON,

    PRIMARY KEY (identifier, load_timestamp)
);

-- ============================================================================
-- Indexes for Bronze Layer
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_bronze_points_identifier
    ON bronze_points(identifier);

CREATE INDEX IF NOT EXISTS idx_bronze_forecast_identifier_time
    ON bronze_forecast_periods(identifier, start_time);

CREATE INDEX IF NOT EXISTS idx_bronze_hourly_identifier_time
    ON bronze_hourly_periods(identifier, start_time);

CREATE INDEX IF NOT EXISTS idx_bronze_observations_identifier_time
    ON bronze_observations(identifier, observation_time);

CREATE INDEX IF NOT EXISTS idx_bronze_stations_identifier
    ON bronze_stations(station_identifier);

CREATE INDEX IF NOT EXISTS idx_bronze_zones_identifier
    ON bronze_zones(zone_id_code);
