"""Data models for weather data pipeline."""

from .api import *

__all__ = [
    # API Models (Pydantic)
    "PointsResponse",
    "ZonesResponse",
    "StationsResponse",
    "GridForecastResponse",
    "HourlyForecastResponse",
    "GridDataResponse",
    "ObservationResponse",
    "ZoneForecastResponse",
    "SkiResort",
    "ResortForecastSnapshot",
]

# Note: Database schema is defined in db/schema.sql (raw SQL)
