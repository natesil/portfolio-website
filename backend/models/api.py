"""
Pydantic models for NOAA weather.gov API responses.

These models represent the data structures from the weather.gov API
and will serve as the foundation for the Data Vault pipeline.
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


# ============================================================================
# Common/Shared Models
# ============================================================================

class QuantitativeValue(BaseModel):
    """Represents a value with unit of measurement."""
    value: Optional[float] = None
    unitCode: Optional[str] = None
    qualityControl: Optional[str] = None


class GeometryPoint(BaseModel):
    """GeoJSON Point geometry."""
    type: str = "Point"
    coordinates: List[float]  # [longitude, latitude, elevation?]


class GeometryPolygon(BaseModel):
    """GeoJSON Polygon geometry."""
    type: str = "Polygon"
    coordinates: List[List[List[float]]]


# ============================================================================
# 1. Points Endpoint Models
# ============================================================================

class PointsProperties(BaseModel):
    """Properties from /points/{lat},{lon} endpoint."""
    id: str = Field(..., alias="@id")
    type: str = Field(..., alias="@type")
    cwa: str  # County Warning Area (office ID)
    forecastOffice: str
    gridId: str
    gridX: int
    gridY: int

    # URLs to other resources
    forecast: str
    forecastHourly: str
    forecastGridData: str
    observationStations: str
    forecastZone: str
    county: str
    fireWeatherZone: str

    # Location details
    timeZone: str
    radarStation: str
    city: Optional[str] = None
    state: Optional[str] = None
    distance: Optional[QuantitativeValue] = None
    bearing: Optional[QuantitativeValue] = None

    relativeLocation: Optional[Dict[str, Any]] = None

    class Config:
        populate_by_name = True


class PointsResponse(BaseModel):
    """Response from /points/{lat},{lon} endpoint."""
    id: str = Field(..., alias="@id")
    type: str = Field(..., alias="@type")
    geometry: GeometryPoint
    properties: PointsProperties

    class Config:
        populate_by_name = True


# ============================================================================
# 2. Zones Models
# ============================================================================

class ZoneProperties(BaseModel):
    """Properties for a forecast zone."""
    id: str = Field(..., alias="@id")
    type: str = Field(..., alias="@type")
    id_code: str = Field(..., alias="id")  # Zone ID like "MEZ007"
    type_code: Optional[str] = Field(None, alias="type")
    name: str
    effectiveDate: datetime
    expirationDate: datetime
    state: Optional[str] = None
    cwa: Optional[List[str]] = None  # County Warning Areas
    forecastOffices: Optional[List[str]] = None
    timeZone: Optional[List[str]] = None
    observationStations: Optional[List[str]] = None
    radarStation: Optional[str] = None

    class Config:
        populate_by_name = True


class ZoneFeature(BaseModel):
    """Single zone feature from zones list."""
    id: str = Field(..., alias="@id")
    type: str = "Feature"
    geometry: Optional[GeometryPolygon] = None
    properties: ZoneProperties

    class Config:
        populate_by_name = True


class ZonesResponse(BaseModel):
    """Response from /zones endpoint."""
    context: Optional[Any] = Field(None, alias="@context")
    type: str = "FeatureCollection"
    features: List[ZoneFeature]
    observationStations: Optional[List[str]] = None
    pagination: Optional[Dict[str, str]] = None

    class Config:
        populate_by_name = True


# ============================================================================
# 3. Stations Models
# ============================================================================

class StationProperties(BaseModel):
    """Properties for an observation station."""
    id: str = Field(..., alias="@id")
    type: str = Field(..., alias="@type")
    elevation: QuantitativeValue
    stationIdentifier: str
    name: str
    timeZone: str
    forecast: Optional[str] = None
    county: Optional[str] = None
    fireWeatherZone: Optional[str] = None

    class Config:
        populate_by_name = True


class StationFeature(BaseModel):
    """Single station feature from stations list."""
    id: str = Field(..., alias="@id")
    type: str = "Feature"
    geometry: GeometryPoint
    properties: StationProperties

    class Config:
        populate_by_name = True


class StationsResponse(BaseModel):
    """Response from /stations endpoint."""
    context: Optional[Any] = Field(None, alias="@context")
    type: str = "FeatureCollection"
    features: List[StationFeature]
    observationStations: List[str]
    pagination: Optional[Dict[str, str]] = None

    class Config:
        populate_by_name = True


# ============================================================================
# 4. Grid Forecast (12-hour periods) Models
# ============================================================================

class ForecastPeriod(BaseModel):
    """Single forecast period (12-hour or hourly)."""
    number: int
    name: str
    startTime: datetime
    endTime: datetime
    isDaytime: bool
    temperature: int
    temperatureUnit: str
    temperatureTrend: Optional[str] = None
    probabilityOfPrecipitation: Optional[QuantitativeValue] = None
    dewpoint: Optional[QuantitativeValue] = None
    relativeHumidity: Optional[QuantitativeValue] = None
    windSpeed: str
    windDirection: str
    icon: str
    shortForecast: str
    detailedForecast: str


class ForecastProperties(BaseModel):
    """Properties from grid forecast endpoint."""
    updateTime: datetime
    units: str
    forecastGenerator: str
    generatedAt: datetime
    updateTime: datetime
    validTimes: str
    elevation: QuantitativeValue
    periods: List[ForecastPeriod]


class GridForecastResponse(BaseModel):
    """Response from /gridpoints/{office}/{x},{y}/forecast endpoint."""
    context: Optional[Any] = Field(None, alias="@context")
    type: str = "Feature"
    geometry: GeometryPolygon
    properties: ForecastProperties

    class Config:
        populate_by_name = True


# ============================================================================
# 5. Hourly Forecast Models (reuses ForecastPeriod and ForecastProperties)
# ============================================================================

class HourlyForecastResponse(BaseModel):
    """Response from /gridpoints/{office}/{x},{y}/forecast/hourly endpoint."""
    context: Optional[Any] = Field(None, alias="@context")
    type: str = "Feature"
    geometry: GeometryPolygon
    properties: ForecastProperties

    class Config:
        populate_by_name = True


# ============================================================================
# 6. Grid Data (Raw Forecast) Models
# ============================================================================

class GridDataValue(BaseModel):
    """Single time-series value from grid data."""
    validTime: str  # ISO8601 duration format
    value: Optional[float] = None


class GridDataLayer(BaseModel):
    """A single data layer (temperature, wind, etc.) with time series."""
    uom: Optional[str] = None  # Unit of measure
    values: List[GridDataValue]


class GridDataProperties(BaseModel):
    """Properties from grid data endpoint - raw numerical forecast data."""

    # Metadata
    updateTime: datetime
    validTimes: str
    elevation: QuantitativeValue
    forecastOffice: Optional[str] = None
    gridId: Optional[str] = None
    gridX: Optional[int] = None
    gridY: Optional[int] = None

    # Weather data layers (all optional, may not all be present)
    temperature: Optional[GridDataLayer] = None
    dewpoint: Optional[GridDataLayer] = None
    maxTemperature: Optional[GridDataLayer] = None
    minTemperature: Optional[GridDataLayer] = None
    relativeHumidity: Optional[GridDataLayer] = None
    apparentTemperature: Optional[GridDataLayer] = None
    heatIndex: Optional[GridDataLayer] = None
    windChill: Optional[GridDataLayer] = None
    skyCover: Optional[GridDataLayer] = None
    windDirection: Optional[GridDataLayer] = None
    windSpeed: Optional[GridDataLayer] = None
    windGust: Optional[GridDataLayer] = None
    weather: Optional[Dict[str, Any]] = None  # Complex nested structure
    hazards: Optional[Dict[str, Any]] = None
    probabilityOfPrecipitation: Optional[GridDataLayer] = None
    quantitativePrecipitation: Optional[GridDataLayer] = None
    iceAccumulation: Optional[GridDataLayer] = None
    snowfallAmount: Optional[GridDataLayer] = None
    snowLevel: Optional[GridDataLayer] = None
    ceilingHeight: Optional[GridDataLayer] = None
    visibility: Optional[GridDataLayer] = None
    transportWindSpeed: Optional[GridDataLayer] = None
    transportWindDirection: Optional[GridDataLayer] = None
    mixingHeight: Optional[GridDataLayer] = None
    hainesIndex: Optional[GridDataLayer] = None
    lightningActivityLevel: Optional[GridDataLayer] = None
    twentyFootWindSpeed: Optional[GridDataLayer] = None
    twentyFootWindDirection: Optional[GridDataLayer] = None
    waveHeight: Optional[GridDataLayer] = None
    wavePeriod: Optional[GridDataLayer] = None
    waveDirection: Optional[GridDataLayer] = None
    primarySwellHeight: Optional[GridDataLayer] = None
    primarySwellDirection: Optional[GridDataLayer] = None
    secondarySwellHeight: Optional[GridDataLayer] = None
    secondarySwellDirection: Optional[GridDataLayer] = None
    wavePeriod2: Optional[GridDataLayer] = None
    windWaveHeight: Optional[GridDataLayer] = None
    dispersionIndex: Optional[GridDataLayer] = None
    pressure: Optional[GridDataLayer] = None
    probabilityOfTropicalStormWinds: Optional[GridDataLayer] = None
    probabilityOfHurricaneWinds: Optional[GridDataLayer] = None
    potentialOf15mphWinds: Optional[GridDataLayer] = None
    potentialOf25mphWinds: Optional[GridDataLayer] = None
    potentialOf35mphWinds: Optional[GridDataLayer] = None
    potentialOf45mphWinds: Optional[GridDataLayer] = None
    potentialOf20mphWindGusts: Optional[GridDataLayer] = None
    potentialOf30mphWindGusts: Optional[GridDataLayer] = None
    potentialOf40mphWindGusts: Optional[GridDataLayer] = None
    potentialOf50mphWindGusts: Optional[GridDataLayer] = None
    potentialOf60mphWindGusts: Optional[GridDataLayer] = None
    grasslandFireDangerIndex: Optional[GridDataLayer] = None
    probabilityOfThunder: Optional[GridDataLayer] = None
    davisStabilityIndex: Optional[GridDataLayer] = None
    atmosphericDispersionIndex: Optional[GridDataLayer] = None
    lowVisibilityOccurrenceRiskIndex: Optional[GridDataLayer] = None
    stability: Optional[GridDataLayer] = None
    redFlagThreatIndex: Optional[GridDataLayer] = None


class GridDataResponse(BaseModel):
    """Response from /gridpoints/{office}/{x},{y} endpoint."""
    context: Optional[Any] = Field(None, alias="@context")
    id: str = Field(..., alias="@id")
    type: str = "Feature"
    geometry: GeometryPolygon
    properties: GridDataProperties

    class Config:
        populate_by_name = True


# ============================================================================
# 7. Station Observations Models
# ============================================================================

class ObservationProperties(BaseModel):
    """Properties from station observation."""
    id: str = Field(..., alias="@id")
    type: str = Field(..., alias="@type")
    elevation: QuantitativeValue
    station: str
    timestamp: datetime
    rawMessage: Optional[str] = None
    textDescription: Optional[str] = None
    icon: Optional[str] = None
    presentWeather: Optional[List[Dict[str, Any]]] = None

    # Observation data
    temperature: Optional[QuantitativeValue] = None
    dewpoint: Optional[QuantitativeValue] = None
    windDirection: Optional[QuantitativeValue] = None
    windSpeed: Optional[QuantitativeValue] = None
    windGust: Optional[QuantitativeValue] = None
    barometricPressure: Optional[QuantitativeValue] = None
    seaLevelPressure: Optional[QuantitativeValue] = None
    visibility: Optional[QuantitativeValue] = None
    maxTemperatureLast24Hours: Optional[QuantitativeValue] = None
    minTemperatureLast24Hours: Optional[QuantitativeValue] = None
    precipitationLastHour: Optional[QuantitativeValue] = None
    precipitationLast3Hours: Optional[QuantitativeValue] = None
    precipitationLast6Hours: Optional[QuantitativeValue] = None
    relativeHumidity: Optional[QuantitativeValue] = None
    windChill: Optional[QuantitativeValue] = None
    heatIndex: Optional[QuantitativeValue] = None
    cloudLayers: Optional[List[Dict[str, Any]]] = None

    class Config:
        populate_by_name = True


class ObservationResponse(BaseModel):
    """Response from /stations/{stationId}/observations/latest endpoint."""
    context: Optional[Any] = Field(None, alias="@context")
    id: str = Field(..., alias="@id")
    type: str = "Feature"
    geometry: GeometryPoint
    properties: ObservationProperties

    class Config:
        populate_by_name = True


# ============================================================================
# 8. Zone Forecast Models
# ============================================================================

class ZoneForecastPeriod(BaseModel):
    """Single period from zone forecast."""
    number: int
    name: str
    detailedForecast: str


class ZoneForecastProperties(BaseModel):
    """Properties from zone forecast."""
    updated: datetime
    periods: List[ZoneForecastPeriod]


class ZoneForecastResponse(BaseModel):
    """Response from /zones/forecast/{zoneId}/forecast endpoint."""
    context: Optional[Any] = Field(None, alias="@context")
    type: str = "Feature"
    geometry: Optional[GeometryPolygon] = None
    properties: ZoneForecastProperties

    class Config:
        populate_by_name = True


# ============================================================================
# Domain Models for Data Vault Pipeline
# ============================================================================

class SkiResort(BaseModel):
    """Ski resort with location and mappings."""
    name: str
    state: str  # ME, NH, VT
    latitude: float
    longitude: float

    # Mapped values from points endpoint
    zone_id: Optional[str] = None
    grid_id: Optional[str] = None
    grid_x: Optional[int] = None
    grid_y: Optional[int] = None
    forecast_office: Optional[str] = None
    nearest_stations: Optional[List[str]] = None


class ResortForecastSnapshot(BaseModel):
    """Complete forecast snapshot for a resort at a point in time."""
    resort_name: str
    snapshot_time: datetime

    # Forecast data
    grid_forecast: Optional[GridForecastResponse] = None
    hourly_forecast: Optional[HourlyForecastResponse] = None
    grid_data: Optional[GridDataResponse] = None

    # Current observations from nearest station
    current_observation: Optional[ObservationResponse] = None
    observation_station_id: Optional[str] = None
