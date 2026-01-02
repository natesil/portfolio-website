# Weather Data Model (dbt)

Data transformation pipeline using dbt + DuckDB to transform raw weather data into a Data Vault 2.0 schema.

## Architecture

```
Raw JSON (datalake/raw/)
    ↓
Bronze Layer (flattened tables)
    ↓
Silver Layer (Data Vault 2.0)
    ├── Hubs (business keys)
    ├── Links (relationships)
    └── Satellites (descriptive data)
    ↓
Gold Layer (analytics views)
```

## Structure

```
models/
├── bronze/              # Flatten raw JSON files
│   ├── bronze_points.sql
│   ├── bronze_forecast_periods.sql
│   ├── bronze_hourly_periods.sql
│   ├── bronze_observations.sql
│   ├── bronze_stations.sql
│   ├── bronze_zones.sql
│   └── bronze_grid_data.sql
│
├── silver/              # Data Vault 2.0
│   ├── hubs/
│   │   ├── hub_resort.sql
│   │   ├── hub_zone.sql
│   │   ├── hub_station.sql
│   │   └── hub_office.sql
│   ├── links/
│   │   ├── link_resort_zone.sql
│   │   ├── link_resort_station.sql
│   │   └── link_zone_office.sql
│   └── satellites/
│       ├── sat_resort_details.sql
│       ├── sat_zone_details.sql
│       ├── sat_station_details.sql
│       ├── sat_office_details.sql
│       ├── sat_forecast_period.sql
│       ├── sat_forecast_hourly.sql
│       ├── sat_observation.sql
│       └── sat_grid_data.sql
│
└── gold/                # Analytics views (TODO)
```

## Usage

### First Time Setup

```bash
# From backend/db/data_model/
cd db/data_model

# Debug and compile models (doesn't run them)
dbt debug
dbt compile
```

### Run Transformations

```bash
# Run all models
dbt run

# Run specific layers
dbt run --select bronze.*
dbt run --select silver.*
dbt run --select silver.hubs.*
dbt run --select silver.links.*
dbt run --select silver.satellites.*

# Run specific model
dbt run --select bronze_points
dbt run --select hub_resort

# Full refresh (rebuild from scratch)
dbt run --full-refresh
```

### Testing

```bash
# Run all tests
dbt test

# Test specific model
dbt test --select hub_resort
```

### Documentation

```bash
# Generate and serve docs
dbt docs generate
dbt docs serve
```

## Model Details

### Bronze Layer
- **Materialization**: Table
- **Source**: Raw JSON files in `../datalake/raw/`
- **Purpose**: Flatten nested JSON structures for easier querying

### Silver Layer (Data Vault)
- **Materialization**: Incremental (only insert new records)
- **Purpose**: Historized, normalized data warehouse

#### Hubs
Core business entities with MD5 hash keys:
- `hub_resort`: Ski resort identifiers
- `hub_zone`: Weather forecast zones
- `hub_station`: Weather observation stations
- `hub_office`: Forecast office identifiers

#### Links
Relationships between hubs:
- `link_resort_zone`: Resort → Zone relationship
- `link_resort_station`: Resort → Station relationship (with rank)
- `link_zone_office`: Zone → Office relationship

#### Satellites
Descriptive and time-series data:
- `sat_*_details`: Descriptive attributes for each hub
- `sat_forecast_period`: 12-hour forecast periods
- `sat_forecast_hourly`: Hourly forecasts
- `sat_observation`: Station observations
- `sat_grid_data`: Raw numerical forecast grid data

## Configuration

### Database
- **Type**: DuckDB
- **Path**: `../data/weather.duckdb`
- **Extensions**: httpfs, parquet

### Schemas
- `bronze`: Bronze layer tables
- `silver`: Silver layer (Data Vault) tables
- `gold`: Gold layer views

## Dependencies

Bronze models must run before Silver:
- Bronze → Silver Hubs → Silver Links → Silver Satellites

dbt automatically handles dependency ordering based on `{{ ref() }}` macros.

## Notes

- All Silver models are **incremental** - they only insert new records
- Hash keys use MD5 for deterministic generation
- Use `--full-refresh` to rebuild incremental models from scratch
- Raw JSON files must exist in `../datalake/raw/` before running Bronze models
