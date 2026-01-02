"""
Data lake writer for landing raw API data.

Saves API responses as JSON files in organized directory structure.
"""

import json
import os
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List


# Get data lake root from environment or use default
DATALAKE_ROOT = os.getenv("DATALAKE_PATH", "datalake/raw")


def save_raw_data(
    category: str,
    data: Dict[str, Any],
    identifier: str,
    timestamp: datetime = None
) -> Path:
    """
    Save raw API response to data lake.

    Args:
        category: Data category (e.g., 'forecasts', 'observations', 'points')
        data: Dictionary to save (typically API response)
        identifier: Unique identifier (e.g., resort name, station ID)
        timestamp: Optional timestamp (defaults to now)

    Returns:
        Path to saved file

    Example:
        >>> save_raw_data(
        ...     category='forecasts',
        ...     data=forecast_response,
        ...     identifier='Sugarloaf'
        ... )
        Path('datalake/raw/forecasts/Sugarloaf_2025-12-30T12-00-00.json')
    """
    if timestamp is None:
        timestamp = datetime.now()

    # Create category directory
    category_dir = Path(DATALAKE_ROOT) / category
    category_dir.mkdir(parents=True, exist_ok=True)

    # Generate filename with timestamp
    timestamp_str = timestamp.strftime("%Y-%m-%dT%H-%M-%S")
    filename = f"{identifier}_{timestamp_str}.json"
    filepath = category_dir / filename

    # Add metadata
    data_with_metadata = {
        "metadata": {
            "saved_at": datetime.now().isoformat(),
            "category": category,
            "identifier": identifier,
            "timestamp": timestamp.isoformat()
        },
        "data": data
    }

    # Save JSON
    with open(filepath, 'w') as f:
        json.dump(data_with_metadata, f, indent=2, default=str)

    return filepath


def load_raw_data(filepath: Path) -> Dict[str, Any]:
    """
    Load raw data from data lake.

    Args:
        filepath: Path to JSON file

    Returns:
        Dictionary with data (excludes metadata wrapper)
    """
    with open(filepath, 'r') as f:
        full_data = json.load(f)

    return full_data.get('data', full_data)


def list_raw_files(
    category: str,
    identifier: str = None,
    start_date: datetime = None,
    end_date: datetime = None
) -> List[Path]:
    """
    List raw data files in data lake.

    Args:
        category: Data category to search
        identifier: Optional filter by identifier
        start_date: Optional filter by timestamp (inclusive)
        end_date: Optional filter by timestamp (inclusive)

    Returns:
        List of matching file paths, sorted by timestamp (newest first)

    Example:
        >>> # Get all forecasts for Sugarloaf
        >>> files = list_raw_files('forecasts', identifier='Sugarloaf')

        >>> # Get forecasts from last week
        >>> files = list_raw_files('forecasts', start_date=week_ago)
    """
    category_dir = Path(DATALAKE_ROOT) / category

    if not category_dir.exists():
        return []

    # Get all JSON files
    all_files = list(category_dir.glob("*.json"))

    # Filter by identifier if specified
    if identifier:
        all_files = [f for f in all_files if f.name.startswith(f"{identifier}_")]

    # Filter by date range if specified
    if start_date or end_date:
        filtered_files = []
        for filepath in all_files:
            try:
                # Extract timestamp from filename
                # Format: identifier_2025-12-30T12-00-00.json
                timestamp_part = filepath.stem.split('_', 1)[1]
                file_timestamp = datetime.strptime(timestamp_part, "%Y-%m-%dT%H-%M-%S")

                # Check date range
                if start_date and file_timestamp < start_date:
                    continue
                if end_date and file_timestamp > end_date:
                    continue

                filtered_files.append(filepath)
            except (ValueError, IndexError):
                # Skip files that don't match expected format
                continue

        all_files = filtered_files

    # Sort by timestamp (newest first)
    all_files.sort(reverse=True)

    return all_files


def get_latest_raw_file(category: str, identifier: str) -> Path:
    """
    Get the most recent raw data file for an identifier.

    Args:
        category: Data category
        identifier: Identifier to search for

    Returns:
        Path to most recent file, or None if not found
    """
    files = list_raw_files(category, identifier=identifier)
    return files[0] if files else None


def save_resort_data(resort_name: str, lat: float, lon: float, client) -> dict:
    """
    Fetch and save all data for a resort.

    Args:
        resort_name: Resort identifier
        lat: Latitude
        lon: Longitude
        client: WeatherClient instance

    Returns:
        Dictionary of saved file paths by category
    """
    timestamp = datetime.now()
    saved_files = {}

    # 1. Get points (grid metadata)
    print(f"  → Fetching points...")
    points = client.get_points(lat, lon)
    saved_files['points'] = save_raw_data(
        'points',
        points.model_dump(),
        resort_name,
        timestamp
    )

    # 2. Get forecasts (12-hour periods)
    print(f"  → Fetching 12-hour forecast...")
    forecast = client.get_forecast_from_points(points)
    saved_files['forecasts'] = save_raw_data(
        'forecasts',
        forecast.model_dump(),
        resort_name,
        timestamp
    )

    # 3. Get hourly forecast
    print(f"  → Fetching hourly forecast...")
    hourly = client.get_hourly_forecast_from_points(points)
    saved_files['hourly'] = save_raw_data(
        'hourly',
        hourly.model_dump(),
        resort_name,
        timestamp
    )

    # 4. Get grid data (raw numerical forecasts)
    print(f"  → Fetching grid data...")
    try:
        grid_data = client.get_grid_data_from_points(points)
        saved_files['grid_data'] = save_raw_data(
            'grid_data',
            grid_data.model_dump(),
            resort_name,
            timestamp
        )
    except Exception as e:
        print(f"    ⚠ Grid data failed: {e}")

    # 5. Get nearest observation stations
    print(f"  → Fetching observation stations...")
    try:
        # Get stations from the observationStations endpoint
        stations_url = points.properties.observationStations
        endpoint = stations_url.replace(client.BASE_URL, "")
        stations_data = client._get(endpoint)

        saved_files['stations'] = save_raw_data(
            'stations',
            stations_data,
            resort_name,
            timestamp
        )

        # Extract station IDs for getting observations
        station_ids = [
            feature["properties"]["stationIdentifier"]
            for feature in stations_data.get("features", [])
        ]

    except Exception as e:
        print(f"    ⚠ Stations failed: {e}")
        station_ids = []

    # 6. Get current observation from nearest station
    if station_ids:
        print(f"  → Fetching observation from {station_ids[0]}...")
        try:
            observation = client.get_station_observation(station_ids[0])
            saved_files['observations'] = save_raw_data(
                'observations',
                observation.model_dump(),
                resort_name,
                timestamp
            )
        except Exception as e:
            print(f"    ⚠ Observation failed: {e}")

    # 7. Get zone data (forecast zone)
    print(f"  → Fetching zone data...")
    try:
        # Extract zone ID from forecastZone URL
        zone_url = points.properties.forecastZone
        zone_id = zone_url.split('/')[-1]  # e.g., "MEZ008"

        # Get zone details by fetching the specific zone
        zone_data = client._get(f"/zones/forecast/{zone_id}")
        saved_files['zones'] = save_raw_data(
            'zones',
            zone_data,
            resort_name,
            timestamp
        )
    except Exception as e:
        print(f"    ⚠ Zone data failed: {e}")

    return saved_files


def collect_all_resorts(client) -> dict:
    """
    Collect data for all resorts in config.

    Args:
        client: WeatherClient instance

    Returns:
        Dictionary mapping resort name to saved files
    """
    from config import load_resorts_config

    config = load_resorts_config()
    results = {}

    for resort in config.resorts:
        print(f"Collecting {resort.name}...")
        try:
            saved = save_resort_data(
                resort.name,
                resort.location.latitude,
                resort.location.longitude,
                client
            )
            results[resort.name] = saved
            print(f"  ✓ Saved {len(saved)} files")
        except Exception as e:
            print(f"  ✗ Failed: {e}")
            results[resort.name] = {}

    return results


if __name__ == "__main__":
    # Test by collecting data for all resorts
    from clients import WeatherClient

    print("Collecting data for all resorts...\n")

    client = WeatherClient()
    results = collect_all_resorts(client)

    total = sum(len(files) for files in results.values())
    print(f"\n✓ Saved {total} files total")
