"""API clients for external services."""

from .weather import WeatherClient, WeatherAPIError

__all__ = ["WeatherClient", "WeatherAPIError"]
