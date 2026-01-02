"""
Bronze Layer Loader

Loads raw JSON files from datalake/raw/ into Bronze tables using DuckDB's read_json().
"""

from pathlib import Path
import duckdb

from db.session import get_connection


def init_bronze_schema(conn: duckdb.DuckDBPyConnection = None):
    """Initialize Bronze layer schema."""
    if conn is None:
        conn = get_connection()

    schema_path = Path(__file__).parent / "bronze_schema.sql"
    with open(schema_path, 'r') as f:
        conn.execute(f.read())

    print("✓ Bronze schema initialized")


def load_points(raw_dir: Path = None, conn: duckdb.DuckDBPyConnection = None) -> int:
    """Load points JSON files into bronze_points."""
    if conn is None:
        conn = get_connection()
    if raw_dir is None:
        raw_dir = Path("datalake/raw")

    json_pattern = str(raw_dir / "points" / "*.json")

    result = conn.execute(f"""
        INSERT INTO bronze_points
        SELECT
            metadata.saved_at::TIMESTAMP,
            metadata.category,
            metadata.identifier,
            metadata.timestamp::TIMESTAMP,
            data.properties.id,
            data.properties.type,
            data.properties.cwa,
            data.properties.forecastOffice,
            data.properties.gridId,
            data.properties.gridX,
            data.properties.gridY,
            data.properties.forecast,
            data.properties.forecastHourly,
            data.properties.forecastGridData,
            data.properties.observationStations,
            data.properties.forecastZone,
            data.properties.county,
            data.properties.fireWeatherZone,
            data.properties.timeZone,
            data.properties.radarStation,
            data.properties.city,
            data.properties.state,
            data.geometry.type,
            data.geometry.coordinates[1],
            data.geometry.coordinates[2],
            data.geometry.coordinates[3]
        FROM read_json('{json_pattern}', auto_detect=true, union_by_name=true)
        RETURNING *
    """)

    return len(result.fetchall())


def load_forecasts(raw_dir: Path = None, conn: duckdb.DuckDBPyConnection = None) -> int:
    """Load forecast JSON files into bronze_forecast_periods."""
    if conn is None:
        conn = get_connection()
    if raw_dir is None:
        raw_dir = Path("datalake/raw")

    json_pattern = str(raw_dir / "forecasts" / "*.json")

    result = conn.execute(f"""
        INSERT INTO bronze_forecast_periods
        SELECT
            metadata.saved_at::TIMESTAMP,
            metadata.category,
            metadata.identifier,
            metadata.timestamp::TIMESTAMP,
            data.properties.generatedAt::TIMESTAMP,
            data.properties.updateTime::TIMESTAMP,
            data.properties.units,
            data.properties.forecastGenerator,
            data.properties.elevation.value,
            data.properties.elevation.unitCode,
            period.number,
            period.name,
            period.startTime::TIMESTAMP,
            period.endTime::TIMESTAMP,
            period.isDaytime,
            period.temperature,
            period.temperatureUnit,
            period.temperatureTrend,
            period.windSpeed,
            period.windDirection,
            period.icon,
            period.shortForecast,
            period.detailedForecast,
            period.probabilityOfPrecipitation.value,
            period.probabilityOfPrecipitation.unitCode,
            period.dewpoint.value,
            period.dewpoint.unitCode,
            period.relativeHumidity.value,
            period.relativeHumidity.unitCode
        FROM read_json('{json_pattern}', auto_detect=true, union_by_name=true),
        UNNEST(data.properties.periods) AS t(period)
        RETURNING *
    """)

    return len(result.fetchall())


def load_hourly(raw_dir: Path = None, conn: duckdb.DuckDBPyConnection = None) -> int:
    """Load hourly forecast JSON files into bronze_hourly_periods."""
    if conn is None:
        conn = get_connection()
    if raw_dir is None:
        raw_dir = Path("datalake/raw")

    json_pattern = str(raw_dir / "hourly" / "*.json")

    result = conn.execute(f"""
        INSERT INTO bronze_hourly_periods
        SELECT
            metadata.saved_at::TIMESTAMP,
            metadata.category,
            metadata.identifier,
            metadata.timestamp::TIMESTAMP,
            data.properties.generatedAt::TIMESTAMP,
            data.properties.updateTime::TIMESTAMP,
            data.properties.units,
            data.properties.forecastGenerator,
            data.properties.elevation.value,
            data.properties.elevation.unitCode,
            period.number,
            period.name,
            period.startTime::TIMESTAMP,
            period.endTime::TIMESTAMP,
            period.isDaytime,
            period.temperature,
            period.temperatureUnit,
            period.temperatureTrend,
            period.windSpeed,
            period.windDirection,
            period.icon,
            period.shortForecast,
            period.detailedForecast,
            period.probabilityOfPrecipitation.value,
            period.probabilityOfPrecipitation.unitCode,
            period.dewpoint.value,
            period.dewpoint.unitCode,
            period.relativeHumidity.value,
            period.relativeHumidity.unitCode
        FROM read_json('{json_pattern}', auto_detect=true, union_by_name=true),
        UNNEST(data.properties.periods) AS t(period)
        RETURNING *
    """)

    return len(result.fetchall())


def load_observations(raw_dir: Path = None, conn: duckdb.DuckDBPyConnection = None) -> int:
    """Load observation JSON files into bronze_observations."""
    if conn is None:
        conn = get_connection()
    if raw_dir is None:
        raw_dir = Path("datalake/raw")

    json_pattern = str(raw_dir / "observations" / "*.json")

    result = conn.execute(f"""
        INSERT INTO bronze_observations
        SELECT
            metadata.saved_at::TIMESTAMP,
            metadata.category,
            metadata.identifier,
            metadata.timestamp::TIMESTAMP,
            data.properties.id,
            data.properties.type,
            data.properties.station,
            REGEXP_EXTRACT(data.properties.station, '[^/]+$'),
            data.properties.timestamp::TIMESTAMP,
            data.properties.rawMessage,
            data.properties.textDescription,
            data.properties.icon,
            data.geometry.type,
            data.geometry.coordinates[1],
            data.geometry.coordinates[2],
            data.properties.elevation.value,
            data.properties.elevation.unitCode,
            data.properties.temperature.value,
            data.properties.temperature.unitCode,
            data.properties.dewpoint.value,
            data.properties.dewpoint.unitCode,
            data.properties.heatIndex.value,
            data.properties.heatIndex.unitCode,
            data.properties.windChill.value,
            data.properties.windChill.unitCode,
            data.properties.maxTemperatureLast24Hours.value,
            data.properties.maxTemperatureLast24Hours.unitCode,
            data.properties.minTemperatureLast24Hours.value,
            data.properties.minTemperatureLast24Hours.unitCode,
            data.properties.windDirection.value,
            data.properties.windDirection.unitCode,
            data.properties.windSpeed.value,
            data.properties.windSpeed.unitCode,
            data.properties.windGust.value,
            data.properties.windGust.unitCode,
            data.properties.barometricPressure.value,
            data.properties.barometricPressure.unitCode,
            data.properties.seaLevelPressure.value,
            data.properties.seaLevelPressure.unitCode,
            data.properties.precipitationLastHour.value,
            data.properties.precipitationLastHour.unitCode,
            data.properties.precipitationLast3Hours.value,
            data.properties.precipitationLast3Hours.unitCode,
            data.properties.precipitationLast6Hours.value,
            data.properties.precipitationLast6Hours.unitCode,
            data.properties.visibility.value,
            data.properties.visibility.unitCode,
            data.properties.relativeHumidity.value,
            data.properties.relativeHumidity.unitCode
        FROM read_json('{json_pattern}', auto_detect=true, union_by_name=true)
        RETURNING *
    """)

    return len(result.fetchall())


def load_stations(raw_dir: Path = None, conn: duckdb.DuckDBPyConnection = None) -> int:
    """Load station JSON files into bronze_stations."""
    if conn is None:
        conn = get_connection()
    if raw_dir is None:
        raw_dir = Path("datalake/raw")

    json_pattern = str(raw_dir / "stations" / "*.json")

    result = conn.execute(f"""
        INSERT INTO bronze_stations
        SELECT
            metadata.saved_at::TIMESTAMP,
            metadata.category,
            metadata.identifier,
            metadata.timestamp::TIMESTAMP,
            feature.properties.id,
            feature.properties.type,
            feature.properties.stationIdentifier,
            feature.properties.name,
            feature.properties.timeZone,
            feature.geometry.type,
            feature.geometry.coordinates[1],
            feature.geometry.coordinates[2],
            feature.properties.elevation.value,
            feature.properties.elevation.unitCode,
            feature.properties.forecast,
            feature.properties.county,
            feature.properties.fireWeatherZone
        FROM read_json('{json_pattern}', auto_detect=true, union_by_name=true),
        UNNEST(data.features) AS t(feature)
        RETURNING *
    """)

    return len(result.fetchall())


def load_zones(raw_dir: Path = None, conn: duckdb.DuckDBPyConnection = None) -> int:
    """Load zone JSON files into bronze_zones."""
    if conn is None:
        conn = get_connection()
    if raw_dir is None:
        raw_dir = Path("datalake/raw")

    json_pattern = str(raw_dir / "zones" / "*.json")

    result = conn.execute(f"""
        INSERT INTO bronze_zones
        SELECT
            metadata.saved_at::TIMESTAMP,
            metadata.category,
            metadata.identifier,
            metadata.timestamp::TIMESTAMP,
            feature.properties.id,
            feature.properties.type,
            feature.properties.id_code,
            feature.properties.type_code,
            feature.properties.name,
            feature.properties.state,
            feature.properties.effectiveDate::TIMESTAMP,
            feature.properties.expirationDate::TIMESTAMP,
            feature.properties.cwa,
            feature.properties.forecastOffices,
            feature.properties.timeZone,
            feature.properties.observationStations,
            feature.properties.radarStation,
            feature.geometry.type,
            feature.geometry.coordinates
        FROM read_json('{json_pattern}', auto_detect=true, union_by_name=true),
        UNNEST(data.features) AS t(feature)
        RETURNING *
    """)

    return len(result.fetchall())


def load_grid_data(raw_dir: Path = None, conn: duckdb.DuckDBPyConnection = None) -> int:
    """Load grid data JSON files into bronze_grid_data."""
    if conn is None:
        conn = get_connection()
    if raw_dir is None:
        raw_dir = Path("datalake/raw")

    json_pattern = str(raw_dir / "grid_data" / "*.json")

    result = conn.execute(f"""
        INSERT INTO bronze_grid_data
        SELECT
            metadata.saved_at::TIMESTAMP,
            metadata.category,
            metadata.identifier,
            metadata.timestamp::TIMESTAMP,
            data.properties.gridId,
            data.properties.gridX,
            data.properties.gridY,
            data.properties.forecastOffice,
            data.properties.updateTime::TIMESTAMP,
            data.properties.validTimes,
            data.properties.elevation.value,
            data.properties.elevation.unitCode,
            data.geometry.type,
            data.geometry.coordinates,
            data.properties.temperature,
            data.properties.dewpoint,
            data.properties.maxTemperature,
            data.properties.minTemperature,
            data.properties.relativeHumidity,
            data.properties.apparentTemperature,
            data.properties.heatIndex,
            data.properties.windChill,
            data.properties.skyCover,
            data.properties.windDirection,
            data.properties.windSpeed,
            data.properties.windGust,
            data.properties.probabilityOfPrecipitation,
            data.properties.quantitativePrecipitation,
            data.properties.iceAccumulation,
            data.properties.snowfallAmount,
            data.properties.snowLevel,
            data.properties.ceilingHeight,
            data.properties.visibility,
            data.properties.weather,
            data.properties.hazards
        FROM read_json('{json_pattern}', auto_detect=true, union_by_name=true)
        RETURNING *
    """)

    return len(result.fetchall())


def load_all_raw_data(raw_dir: Path = None, conn: duckdb.DuckDBPyConnection = None):
    """Load all raw data into Bronze tables."""
    if conn is None:
        conn = get_connection()
    if raw_dir is None:
        raw_dir = Path("datalake/raw")

    print("Loading raw data into Bronze layer...\n")

    loaders = [
        ("points", load_points),
        ("forecasts", load_forecasts),
        ("hourly", load_hourly),
        ("observations", load_observations),
        ("stations", load_stations),
        ("zones", load_zones),
        ("grid_data", load_grid_data),
    ]

    total_rows = 0
    for name, loader_func in loaders:
        print(f"Loading {name}...")
        try:
            rows = loader_func(raw_dir, conn)
            total_rows += rows
            print(f"  ✓ {rows} rows\n")
        except Exception as e:
            print(f"  ✗ Failed: {e}\n")

    print(f"✓ Loaded {total_rows} total rows into Bronze layer")
    return total_rows


if __name__ == "__main__":
    conn = get_connection()

    # Clear existing bronze tables
    print("Clearing existing Bronze tables...")
    conn.execute("DELETE FROM bronze_hourly_periods")
    conn.execute("DELETE FROM bronze_forecast_periods")
    conn.execute("DELETE FROM bronze_points")
    conn.execute("DELETE FROM bronze_observations")
    conn.execute("DELETE FROM bronze_stations")
    conn.execute("DELETE FROM bronze_zones")
    conn.execute("DELETE FROM bronze_grid_data")
    print("✓ Tables cleared\n")

    init_bronze_schema(conn)
    print()
    load_all_raw_data(conn=conn)
