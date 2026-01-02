# Data Lake

Raw data storage for weather API responses before transformation into Data Vault.

## Architecture

```
┌─────────────────┐
│  weather.gov    │
│   API           │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Data Lake     │ ← YOU ARE HERE
│   (Raw/Bronze)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Data Vault    │
│   (Silver)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Analytics Views│
│   (Gold)        │
└─────────────────┘
```

## Directory Structure

```
datalake/
└── raw/                    # Raw API responses (Bronze layer)
    ├── points/             # /points endpoint responses
    ├── forecasts/          # 12-hour forecast responses
    ├── hourly/             # Hourly forecast responses
    ├── grid_data/          # Raw grid data responses
    ├── observations/       # Station observation responses
    ├── zones/              # Zone information responses
    └── stations/           # Station information responses
```

## File Naming Convention

Files are named with identifier and timestamp:

```
{identifier}_{timestamp}.json

Examples:
- Sugarloaf_2025-12-30T12-00-00.json
- Stratton_2025-12-30T12-00-00.json
- KPWM_2025-12-30T12-30-15.json
```

## File Format

Each JSON file contains:
- **metadata**: Save timestamp, category, identifier
- **data**: Raw API response (preserved exactly as received)

```json
{
  "metadata": {
    "saved_at": "2025-12-30T12:00:00",
    "category": "forecasts",
    "identifier": "Sugarloaf",
    "timestamp": "2025-12-30T12:00:00"
  },
  "data": {
    "@context": "...",
    "type": "Feature",
    "properties": {
      "periods": [...]
    }
  }
}
```

## Usage

### Save Raw Data

```python
from datalake import save_raw_data
from clients import WeatherClient

client = WeatherClient()

# Get forecast
forecast = client.get_forecast_from_points(points)

# Save to data lake
filepath = save_raw_data(
    category='forecasts',
    data=forecast.dict(),  # Convert Pydantic to dict
    identifier='Sugarloaf'
)
```

### Load Raw Data

```python
from datalake import load_raw_data, get_latest_raw_file

# Get latest forecast for a resort
filepath = get_latest_raw_file('forecasts', 'Sugarloaf')
data = load_raw_data(filepath)
```

### List Files

```python
from datalake import list_raw_files
from datetime import datetime, timedelta

# Get all forecasts for Sugarloaf
files = list_raw_files('forecasts', identifier='Sugarloaf')

# Get forecasts from last 7 days
week_ago = datetime.now() - timedelta(days=7)
recent_files = list_raw_files('forecasts', start_date=week_ago)
```

## Benefits

1. **Auditability** - Keep exact API responses as received
2. **Replayability** - Can reprocess data with different logic
3. **Debugging** - Easy to see what API actually returned
4. **Compliance** - Raw data preserved for auditing
5. **Separation** - Landing layer separate from transformation

## ETL Flow

1. **Extract** → Save raw API response to data lake
2. **Transform** → Read from data lake, apply business logic
3. **Load** → Insert into Data Vault (DuckDB)

This allows you to:
- Run ETL multiple times on same raw data
- Change transformation logic without re-fetching from API
- Backfill historical data by reprocessing data lake

## Storage

- **Local Development**: `datalake/raw/` directory
- **Production**: Can point to S3 bucket via `DATALAKE_PATH` env var
  ```bash
  DATALAKE_PATH=s3://my-bucket/weather/raw
  ```

## Cleanup

Old files can be archived or deleted based on retention policy:

```python
# Example: Keep last 30 days, archive older
from pathlib import Path
from datetime import datetime, timedelta

cutoff = datetime.now() - timedelta(days=30)
old_files = list_raw_files('forecasts', end_date=cutoff)

for file in old_files:
    # Archive to S3 or delete
    file.unlink()
```
