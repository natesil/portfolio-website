# Weather Data Pipeline

A modern data pipeline for collecting and storing weather forecasts for New England ski resorts.

## Features

- 🌤️ **NOAA Weather API Integration** - Real-time and 7-day forecasts
- 🏔️ **Ski Resort Focus** - Track weather for major New England ski areas
- 📊 **Data Vault 2.0** - Scalable, auditable data warehouse design
- 🦆 **DuckDB** - Fast, embedded analytical database
- 🔄 **Prefect Orchestration** - Modern workflow management (coming soon)

## Architecture

```
┌─────────────────┐
│  weather.gov    │
│   Forecast API  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Weather Client │
│  (API wrapper)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Prefect Flows  │
│  (Orchestration)│
└────┬───────┬────┘
     │       │
     ▼       ▼
┌─────────────────┐     ┌─────────────────┐
│   Data Lake     │────▶│     DuckDB      │
│  (Raw/Bronze)   │     │  (Data Vault)   │
│  JSON files     │     │  Silver/Gold    │
└─────────────────┘     └─────────────────┘
```

**Data Flow:**
1. **Extract** - Fetch from API → Save raw JSON to Data Lake
2. **Transform** - Read from Data Lake → Apply business logic
3. **Load** - Insert into Data Vault (DuckDB)

## Project Structure

```
backend/
├── config/              # Configuration files
│   └── resorts.yaml     # Ski resort definitions
├── models/              # Data models
│   └── api.py           # Pydantic models (API responses)
├── clients/             # API clients
│   └── weather.py       # NOAA weather.gov client
├── db/                  # Database layer
│   ├── schema.sql       # Data Vault DDL
│   ├── session.py       # Connection & session management
│   └── utils.py         # Hash key generation
├── datalake/            # Raw data storage (Bronze layer)
│   ├── raw/             # Raw API responses (JSON)
│   │   ├── forecasts/
│   │   ├── hourly/
│   │   ├── observations/
│   │   └── ...
│   └── writer.py        # Data lake utilities
├── flows/               # Prefect workflows (coming soon)
│   ├── setup.py         # Initial data vault setup
│   └── collect.py       # Daily forecast collection
├── notebooks/           # Jupyter notebooks
│   └── exploration.ipynb
├── data/                # Database files (gitignored)
│   └── weather.duckdb
├── console.py           # Streamlit data viewer
├── .env                 # Environment variables (gitignored)
├── .env.example         # Example environment config
├── pyproject.toml       # Package configuration
└── requirements.txt     # Python dependencies
```

## Quick Start

### 1. Install Dependencies

```bash
# Using pip
pip install -r requirements.txt

# Or install as editable package
pip install -e .

# Or with all extras
pip install -e ".[all]"
```

### 2. Configure Environment

```bash
# Copy example env file
cp .env.example .env

# Edit .env with your settings
# Mainly: WEATHER_API_USER_AGENT=(your-app, your@email.com)
```

### 3. Initialize Database

```bash
# Run database initialization
python -m db.session

# Or use the CLI command
init-db
```

### 4. Test the API Client

```python
from clients import WeatherClient
from config import load_resorts_config

# Load resorts
config = load_resorts_config()

# Create client
client = WeatherClient()

# Get forecast for a resort
resort = config.resorts[0]
data = client.get_all_forecast_data(
    resort.location.latitude,
    resort.location.longitude
)

print(f"Forecast for {resort.name}:")
print(data['forecast'].properties.periods[0].shortForecast)
```

## Data Model

### Hubs (Business Keys)
- **HubResort** - Ski resorts
- **HubZone** - NWS forecast zones
- **HubStation** - Observation stations
- **HubOffice** - NWS forecast offices

### Links (Relationships)
- **LinkResortZone** - Resort → Zone mapping
- **LinkResortStation** - Resort → Nearest stations
- **LinkZoneOffice** - Zone → Managing office

### Satellites (Time-Variant Data)
- **SatResortDetails** - Resort metadata
- **SatZoneDetails** - Zone metadata
- **SatStationDetails** - Station metadata
- **SatOfficeDetails** - Office metadata
- **SatForecastPeriod** - 12-hour forecast periods
- **SatForecastHourly** - Hourly forecasts
- **SatGridData** - Raw numerical forecast data
- **SatObservation** - Current conditions

## Development

### View Data Vault Console

```bash
# Run Streamlit console to view all tables
streamlit run console.py
```

This opens a web interface showing:
- All Hubs (business keys)
- All Links (relationships)
- All Satellites (descriptive data)
- Row counts and data for each table

### Run Jupyter Notebook

```bash
jupyter notebook notebooks/exploration.ipynb
```

### Code Formatting

```bash
# Format code
black .

# Lint code
ruff check .
```

### Testing

```bash
pytest
```

## Roadmap

- [x] API client implementation
- [x] Data Vault schema design
- [x] DuckDB integration
- [ ] Prefect workflow orchestration
- [ ] Initial data vault setup flow
- [ ] Daily forecast collection flow
- [ ] S3 archival for historical data
- [ ] Semantic text-to-SQL layer
- [ ] Web chatbot integration

## License

MIT
