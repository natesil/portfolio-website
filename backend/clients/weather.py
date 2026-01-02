"""
NOAA weather.gov API client.

Provides clean interface to weather.gov API with Pydantic model validation.
"""

import requests
from typing import List, Optional
from models.api import (
    PointsResponse,
    ZonesResponse,
    StationsResponse,
    GridForecastResponse,
    HourlyForecastResponse,
    GridDataResponse,
    ObservationResponse,
    ZoneForecastResponse,
)


class WeatherAPIError(Exception):
    """Base exception for Weather API errors."""
    pass


class WeatherClient:
    """
    Client for NOAA weather.gov API.

    Usage:
        client = WeatherClient(user_agent="(myapp.com, contact@myapp.com)")
        points = client.get_points(44.4667, -70.8500)
        forecast = client.get_forecast(points)
    """

    BASE_URL = "https://api.weather.gov"

    def __init__(self, user_agent: str = "(portfolio-weather-app, nate@example.com)"):
        """
        Initialize weather client.

        Args:
            user_agent: User-Agent header (required by weather.gov API)
        """
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'application/geo+json'
        })

    def _get(self, endpoint: str, params: Optional[dict] = None) -> dict:
        """
        Make GET request to API.

        Args:
            endpoint: API endpoint (e.g., "/points/44.4667,-70.8500")
            params: Query parameters

        Returns:
            JSON response as dict

        Raises:
            WeatherAPIError: If request fails
        """
        url = f"{self.BASE_URL}{endpoint}"

        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise WeatherAPIError(f"API request failed: {e}") from e

    # ========================================================================
    # Points API
    # ========================================================================

    def get_points(self, latitude: float, longitude: float) -> PointsResponse:
        """
        Get grid/zone metadata for a location.

        This is the starting point - converts lat/lon to grid coordinates,
        zone IDs, and forecast URLs.

        Args:
            latitude: Latitude in decimal degrees
            longitude: Longitude in decimal degrees

        Returns:
            PointsResponse with grid, zone, and forecast metadata
        """
        endpoint = f"/points/{latitude},{longitude}"
        data = self._get(endpoint)
        return PointsResponse(**data)

    # ========================================================================
    # Zones API
    # ========================================================================

    def get_zones(
        self,
        area: Optional[str] = None,
        zone_type: str = "forecast",
        region: Optional[str] = None
    ) -> ZonesResponse:
        """
        Get list of forecast zones.

        Args:
            area: State code (e.g., "ME", "NH", "VT")
            zone_type: Type of zone ("forecast", "fire", "county")
            region: Region filter (e.g., "ER" for Eastern Region)

        Returns:
            ZonesResponse with list of zones
        """
        params = {"type": zone_type}
        if area:
            params["area"] = area
        if region:
            params["region"] = region

        data = self._get("/zones", params=params)
        return ZonesResponse(**data)

    def get_zone_forecast(self, zone_id: str) -> ZoneForecastResponse:
        """
        Get forecast for a specific zone.

        Args:
            zone_id: Zone identifier (e.g., "MEZ007")

        Returns:
            ZoneForecastResponse with zone forecast
        """
        endpoint = f"/zones/forecast/{zone_id}/forecast"
        data = self._get(endpoint)
        return ZoneForecastResponse(**data)

    # ========================================================================
    # Stations API
    # ========================================================================

    def get_stations(
        self,
        state: Optional[str] = None,
        limit: int = 500
    ) -> StationsResponse:
        """
        Get list of observation stations.

        Args:
            state: State code filter (e.g., "ME", "NH", "VT")
            limit: Maximum number of results

        Returns:
            StationsResponse with list of stations
        """
        params = {"limit": limit}
        if state:
            params["state"] = state

        data = self._get("/stations", params=params)
        return StationsResponse(**data)

    def get_station_observation(self, station_id: str) -> ObservationResponse:
        """
        Get latest observation from a station.

        Args:
            station_id: Station identifier (e.g., "KPWM")

        Returns:
            ObservationResponse with current conditions
        """
        endpoint = f"/stations/{station_id}/observations/latest"
        data = self._get(endpoint)
        return ObservationResponse(**data)

    def get_stations_for_point(self, latitude: float, longitude: float) -> List[str]:
        """
        Get list of nearest observation stations for a location.

        Args:
            latitude: Latitude in decimal degrees
            longitude: Longitude in decimal degrees

        Returns:
            List of station IDs, ordered by distance (nearest first)
        """
        # First get the observation stations URL from points
        points = self.get_points(latitude, longitude)
        stations_url = points.properties.observationStations

        # Extract endpoint from full URL
        endpoint = stations_url.replace(self.BASE_URL, "")
        data = self._get(endpoint)

        # Extract station IDs from features
        station_ids = [
            feature["properties"]["stationIdentifier"]
            for feature in data.get("features", [])
        ]

        return station_ids

    # ========================================================================
    # Forecast API
    # ========================================================================

    def get_forecast(
        self,
        office: str,
        grid_x: int,
        grid_y: int
    ) -> GridForecastResponse:
        """
        Get 7-day forecast with 12-hour periods.

        Args:
            office: Forecast office ID (e.g., "GYX")
            grid_x: Grid X coordinate
            grid_y: Grid Y coordinate

        Returns:
            GridForecastResponse with forecast periods
        """
        endpoint = f"/gridpoints/{office}/{grid_x},{grid_y}/forecast"
        data = self._get(endpoint)
        print(data["properties"].keys())
        return GridForecastResponse(**data)

    def get_forecast_from_points(self, points: PointsResponse) -> GridForecastResponse:
        """
        Get forecast using PointsResponse metadata.

        Args:
            points: PointsResponse from get_points()

        Returns:
            GridForecastResponse with forecast periods
        """
        return self.get_forecast(
            office=points.properties.gridId,
            grid_x=points.properties.gridX,
            grid_y=points.properties.gridY
        )

    def get_hourly_forecast(
        self,
        office: str,
        grid_x: int,
        grid_y: int
    ) -> HourlyForecastResponse:
        """
        Get hourly forecast for next 7 days.

        Args:
            office: Forecast office ID (e.g., "GYX")
            grid_x: Grid X coordinate
            grid_y: Grid Y coordinate

        Returns:
            HourlyForecastResponse with hourly periods
        """
        endpoint = f"/gridpoints/{office}/{grid_x},{grid_y}/forecast/hourly"
        data = self._get(endpoint)
        return HourlyForecastResponse(**data)

    def get_hourly_forecast_from_points(
        self,
        points: PointsResponse
    ) -> HourlyForecastResponse:
        """
        Get hourly forecast using PointsResponse metadata.

        Args:
            points: PointsResponse from get_points()

        Returns:
            HourlyForecastResponse with hourly periods
        """
        return self.get_hourly_forecast(
            office=points.properties.gridId,
            grid_x=points.properties.gridX,
            grid_y=points.properties.gridY
        )

    def get_grid_data(
        self,
        office: str,
        grid_x: int,
        grid_y: int
    ) -> GridDataResponse:
        """
        Get raw numerical forecast data.

        Contains 50+ weather data layers including temperature, wind,
        precipitation, snowfall, etc.

        Args:
            office: Forecast office ID (e.g., "GYX")
            grid_x: Grid X coordinate
            grid_y: Grid Y coordinate

        Returns:
            GridDataResponse with all forecast data layers
        """
        endpoint = f"/gridpoints/{office}/{grid_x},{grid_y}"
        data = self._get(endpoint)
        return GridDataResponse(**data)

    def get_grid_data_from_points(self, points: PointsResponse) -> GridDataResponse:
        """
        Get grid data using PointsResponse metadata.

        Args:
            points: PointsResponse from get_points()

        Returns:
            GridDataResponse with all forecast data layers
        """
        return self.get_grid_data(
            office=points.properties.gridId,
            grid_x=points.properties.gridX,
            grid_y=points.properties.gridY
        )

    # ========================================================================
    # Convenience Methods
    # ========================================================================

    def get_all_forecast_data(
        self,
        latitude: float,
        longitude: float
    ) -> dict:
        """
        Get all forecast data for a location in one call.

        Retrieves:
        - Points metadata (grid, zone)
        - 12-hour forecast
        - Hourly forecast
        - Grid data
        - Current observation from nearest station

        Args:
            latitude: Latitude in decimal degrees
            longitude: Longitude in decimal degrees

        Returns:
            Dict with all forecast data
        """
        # Get grid coordinates and zone
        points = self.get_points(latitude, longitude)

        # Get forecasts
        forecast = self.get_forecast_from_points(points)
        hourly = self.get_hourly_forecast_from_points(points)
        grid_data = self.get_grid_data_from_points(points)

        # Get current observation from nearest station
        stations = self.get_stations_for_point(latitude, longitude)
        observation = None
        if stations:
            try:
                observation = self.get_station_observation(stations[0])
            except WeatherAPIError:
                pass  # Station might not have recent data

        return {
            "points": points,
            "forecast": forecast,
            "hourly_forecast": hourly,
            "grid_data": grid_data,
            "observation": observation,
            "nearest_station_id": stations[0] if stations else None
        }


if __name__ == "__main__":
    # Example usage
    client = WeatherClient()

    # Test with Sugarloaf, ME
    print("Testing WeatherClient with Sugarloaf, ME\n")

    # Get points data
    points = client.get_points(45.0317, -70.3139)
    print(f"Grid: {points.properties.gridId} ({points.properties.gridX}, {points.properties.gridY})")
    print(f"Zone: {points.properties.forecastZone.split('/')[-1]}")
    print(f"Office: {points.properties.forecastOffice}")

    # Get forecast
    print("\nGetting 7-day forecast...")
    forecast = client.get_forecast_from_points(points)
    print(f"Found {len(forecast.properties.periods)} forecast periods")
    print(f"Next period: {forecast.properties.periods[0].name}")
    print(f"  {forecast.properties.periods[0].shortForecast}")

    # Get stations
    print("\nGetting nearest stations...")
    stations = client.get_stations_for_point(45.0317, -70.3139)
    print(f"Nearest stations: {stations[:3]}")

    print("\n✓ All tests passed!")
    observations = client.get_station_observation(stations[0])
    print(f"Current conditions at {stations[0]}:")
    print(observations)
