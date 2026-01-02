-- ============================================================================
-- Data Vault 2.0 Schema for Weather Data Pipeline
-- Database: DuckDB
-- ============================================================================

-- ============================================================================
-- HUBS - Business Keys
-- ============================================================================

CREATE TABLE IF NOT EXISTS hub_resort (
    resort_key VARCHAR(32) PRIMARY KEY,
    resort_name VARCHAR(100) NOT NULL UNIQUE,
    load_date TIMESTAMP NOT NULL DEFAULT current_timestamp,
    record_source VARCHAR(50) NOT NULL DEFAULT 'resorts.yaml'
);

CREATE TABLE IF NOT EXISTS hub_zone (
    zone_key VARCHAR(32) PRIMARY KEY,
    zone_id VARCHAR(10) NOT NULL UNIQUE,
    load_date TIMESTAMP NOT NULL DEFAULT current_timestamp,
    record_source VARCHAR(50) NOT NULL DEFAULT 'weather.gov'
);

CREATE TABLE IF NOT EXISTS hub_station (
    station_key VARCHAR(32) PRIMARY KEY,
    station_id VARCHAR(10) NOT NULL UNIQUE,
    load_date TIMESTAMP NOT NULL DEFAULT current_timestamp,
    record_source VARCHAR(50) NOT NULL DEFAULT 'weather.gov'
);

CREATE TABLE IF NOT EXISTS hub_office (
    office_key VARCHAR(32) PRIMARY KEY,
    office_id VARCHAR(10) NOT NULL UNIQUE,
    load_date TIMESTAMP NOT NULL DEFAULT current_timestamp,
    record_source VARCHAR(50) NOT NULL DEFAULT 'weather.gov'
);

-- ============================================================================
-- LINKS - Relationships
-- ============================================================================

CREATE TABLE IF NOT EXISTS link_resort_zone (
    link_key VARCHAR(32) PRIMARY KEY,
    resort_key VARCHAR(32) NOT NULL REFERENCES hub_resort(resort_key),
    zone_key VARCHAR(32) NOT NULL REFERENCES hub_zone(zone_key),
    load_date TIMESTAMP NOT NULL DEFAULT current_timestamp,
    record_source VARCHAR(50) NOT NULL DEFAULT 'weather.gov'
);

CREATE TABLE IF NOT EXISTS link_resort_station (
    link_key VARCHAR(32) PRIMARY KEY,
    resort_key VARCHAR(32) NOT NULL REFERENCES hub_resort(resort_key),
    station_key VARCHAR(32) NOT NULL REFERENCES hub_station(station_key),
    station_rank INTEGER NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT current_timestamp,
    record_source VARCHAR(50) NOT NULL DEFAULT 'weather.gov'
);

CREATE TABLE IF NOT EXISTS link_zone_office (
    link_key VARCHAR(32) PRIMARY KEY,
    zone_key VARCHAR(32) NOT NULL REFERENCES hub_zone(zone_key),
    office_key VARCHAR(32) NOT NULL REFERENCES hub_office(office_key),
    load_date TIMESTAMP NOT NULL DEFAULT current_timestamp,
    record_source VARCHAR(50) NOT NULL DEFAULT 'weather.gov'
);

-- ============================================================================
-- SATELLITES - Descriptive Data
-- ============================================================================

CREATE TABLE IF NOT EXISTS sat_resort_details (
    resort_key VARCHAR(32) NOT NULL REFERENCES hub_resort(resort_key),
    load_date TIMESTAMP NOT NULL DEFAULT current_timestamp,
    load_end_date TIMESTAMP,
    record_source VARCHAR(50) NOT NULL DEFAULT 'resorts.yaml',

    -- Attributes
    state VARCHAR(2) NOT NULL,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    full_name VARCHAR(200),
    region VARCHAR(100),
    grid_id VARCHAR(10),
    grid_x INTEGER,
    grid_y INTEGER,

    PRIMARY KEY (resort_key, load_date)
);

CREATE TABLE IF NOT EXISTS sat_zone_details (
    zone_key VARCHAR(32) NOT NULL REFERENCES hub_zone(zone_key),
    load_date TIMESTAMP NOT NULL DEFAULT current_timestamp,
    load_end_date TIMESTAMP,
    record_source VARCHAR(50) NOT NULL DEFAULT 'weather.gov',

    -- Attributes
    name VARCHAR(200),
    state VARCHAR(2),
    effective_date TIMESTAMP,
    expiration_date TIMESTAMP,
    time_zone VARCHAR(50),

    PRIMARY KEY (zone_key, load_date)
);

CREATE TABLE IF NOT EXISTS sat_station_details (
    station_key VARCHAR(32) NOT NULL REFERENCES hub_station(station_key),
    load_date TIMESTAMP NOT NULL DEFAULT current_timestamp,
    load_end_date TIMESTAMP,
    record_source VARCHAR(50) NOT NULL DEFAULT 'weather.gov',

    -- Attributes
    name VARCHAR(200),
    latitude DOUBLE,
    longitude DOUBLE,
    elevation_value DOUBLE,
    elevation_unit VARCHAR(50),
    time_zone VARCHAR(50),

    PRIMARY KEY (station_key, load_date)
);

CREATE TABLE IF NOT EXISTS sat_office_details (
    office_key VARCHAR(32) NOT NULL REFERENCES hub_office(office_key),
    load_date TIMESTAMP NOT NULL DEFAULT current_timestamp,
    load_end_date TIMESTAMP,
    record_source VARCHAR(50) NOT NULL DEFAULT 'weather.gov',

    -- Attributes
    name VARCHAR(200),
    url VARCHAR(500),

    PRIMARY KEY (office_key, load_date)
);

-- ============================================================================
-- SATELLITES - Time-Series Forecast Data
-- ============================================================================

CREATE TABLE IF NOT EXISTS sat_forecast_period (
    resort_key VARCHAR(32) NOT NULL REFERENCES hub_resort(resort_key),
    load_date TIMESTAMP NOT NULL DEFAULT current_timestamp,
    record_source VARCHAR(50) NOT NULL DEFAULT 'weather.gov',

    -- Forecast metadata
    forecast_generated_at TIMESTAMP NOT NULL,
    forecast_updated_at TIMESTAMP NOT NULL,

    -- Period identification
    period_number INTEGER NOT NULL,
    period_name VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    is_daytime BOOLEAN NOT NULL,

    -- Forecast values
    temperature INTEGER,
    temperature_unit VARCHAR(1),
    temperature_trend VARCHAR(20),
    wind_speed VARCHAR(50),
    wind_direction VARCHAR(10),
    icon_url VARCHAR(500),
    short_forecast TEXT,
    detailed_forecast TEXT,

    -- Probabilities and measurements
    probability_of_precipitation INTEGER,
    dewpoint_value DOUBLE,
    dewpoint_unit VARCHAR(50),
    relative_humidity INTEGER,

    PRIMARY KEY (resort_key, load_date, period_number)
);

CREATE TABLE IF NOT EXISTS sat_forecast_hourly (
    resort_key VARCHAR(32) NOT NULL REFERENCES hub_resort(resort_key),
    load_date TIMESTAMP NOT NULL DEFAULT current_timestamp,
    record_source VARCHAR(50) NOT NULL DEFAULT 'weather.gov',

    -- Forecast metadata
    forecast_generated_at TIMESTAMP NOT NULL,
    forecast_updated_at TIMESTAMP NOT NULL,

    -- Period identification
    period_number INTEGER NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    is_daytime BOOLEAN NOT NULL,

    -- Forecast values
    temperature INTEGER,
    temperature_unit VARCHAR(1),
    wind_speed VARCHAR(50),
    wind_direction VARCHAR(10),
    icon_url VARCHAR(500),
    short_forecast TEXT,

    -- Probabilities and measurements
    probability_of_precipitation INTEGER,
    dewpoint_value DOUBLE,
    dewpoint_unit VARCHAR(50),
    relative_humidity INTEGER,

    PRIMARY KEY (resort_key, load_date, period_number)
);

CREATE TABLE IF NOT EXISTS sat_grid_data (
    resort_key VARCHAR(32) NOT NULL REFERENCES hub_resort(resort_key),
    load_date TIMESTAMP NOT NULL DEFAULT current_timestamp,
    record_source VARCHAR(50) NOT NULL DEFAULT 'weather.gov',

    -- Forecast metadata
    forecast_updated_at TIMESTAMP NOT NULL,
    valid_time_start TIMESTAMP NOT NULL,
    valid_time_end TIMESTAMP NOT NULL,

    -- Weather data layers (JSON for flexibility)
    temperature_data JSON,
    dewpoint_data JSON,
    wind_speed_data JSON,
    wind_direction_data JSON,
    precipitation_data JSON,
    snowfall_data JSON,
    sky_cover_data JSON,

    PRIMARY KEY (resort_key, load_date, valid_time_start)
);

CREATE TABLE IF NOT EXISTS sat_observation (
    station_key VARCHAR(32) NOT NULL REFERENCES hub_station(station_key),
    load_date TIMESTAMP NOT NULL DEFAULT current_timestamp,
    record_source VARCHAR(50) NOT NULL DEFAULT 'weather.gov',

    -- Observation metadata
    observation_time TIMESTAMP NOT NULL,
    text_description VARCHAR(200),
    icon_url VARCHAR(500),
    raw_message TEXT,

    -- Measurements
    temperature DOUBLE,
    temperature_unit VARCHAR(50),
    dewpoint DOUBLE,
    dewpoint_unit VARCHAR(50),
    wind_direction DOUBLE,
    wind_speed DOUBLE,
    wind_speed_unit VARCHAR(50),
    wind_gust DOUBLE,
    wind_gust_unit VARCHAR(50),
    barometric_pressure DOUBLE,
    barometric_pressure_unit VARCHAR(50),
    visibility DOUBLE,
    visibility_unit VARCHAR(50),
    relative_humidity DOUBLE,
    wind_chill DOUBLE,
    wind_chill_unit VARCHAR(50),
    heat_index DOUBLE,
    heat_index_unit VARCHAR(50),

    -- Precipitation
    precipitation_last_hour DOUBLE,
    precipitation_last_3hours DOUBLE,
    precipitation_last_6hours DOUBLE,
    precipitation_unit VARCHAR(50),

    PRIMARY KEY (station_key, observation_time)
);

-- ============================================================================
-- INDEXES for Performance
-- ============================================================================

-- Forecast lookups by resort and time
CREATE INDEX IF NOT EXISTS idx_forecast_period_resort_time
    ON sat_forecast_period(resort_key, start_time);

CREATE INDEX IF NOT EXISTS idx_forecast_hourly_resort_time
    ON sat_forecast_hourly(resort_key, start_time);

-- Observation lookups by station and time
CREATE INDEX IF NOT EXISTS idx_observation_station_time
    ON sat_observation(station_key, observation_time);

-- Link lookups
CREATE INDEX IF NOT EXISTS idx_link_resort_zone_resort
    ON link_resort_zone(resort_key);

CREATE INDEX IF NOT EXISTS idx_link_resort_station_resort
    ON link_resort_station(resort_key);
