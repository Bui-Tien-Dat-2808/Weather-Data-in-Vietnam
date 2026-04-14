"""
Domain entities for the Weather Data Pipeline.
These represent core business objects independent of implementation details.
"""
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, Dict, Any


@dataclass
class WeatherData:
    """
    Domain entity representing weather data for a city.
    This is a pure business object with no database or API dependencies.
    """
    city: str
    timestamp: datetime
    temperature: float
    humidity: int
    wind_speed: float
    description: Optional[str] = None
    pressure: Optional[int] = None
    clouds: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert entity to dictionary."""
        return asdict(self)

    def is_valid(self, 
                 min_temp: float = -60.0,
                 max_temp: float = 60.0,
                 min_humidity: int = 0,
                 max_humidity: int = 100,
                 min_wind: float = 0.0,
                 max_wind: float = 150.0) -> bool:
        """
        Validate weather data against reasonable constraints.

        Args:
            min_temp: Minimum valid temperature in Celsius
            max_temp: Maximum valid temperature in Celsius
            min_humidity: Minimum valid humidity in percentage
            max_humidity: Maximum valid humidity in percentage
            min_wind: Minimum valid wind speed in m/s
            max_wind: Maximum valid wind speed in m/s

        Returns:
            True if all values are within valid ranges
        """
        return (
            min_temp <= self.temperature <= max_temp and
            min_humidity <= self.humidity <= max_humidity and
            min_wind <= self.wind_speed <= max_wind
        )


@dataclass
class City:
    """
    Domain entity representing a city location.
    """
    name: str
    country: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert entity to dictionary."""
        return asdict(self)


@dataclass
class WeatherRecord:
    """
    Domain entity representing a complete weather record with city information.
    """
    city_name: str
    country: str
    timestamp: datetime
    temperature: float
    humidity: int
    wind_speed: float
    description: Optional[str] = None
    pressure: Optional[int] = None
    clouds: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert entity to dictionary."""
        return asdict(self)
