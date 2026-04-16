"""
Data cleaning and validation service.
Handles cleaning, validation, and normalization of weather data.
"""
import logging
from typing import List, Optional, Tuple
from datetime import datetime

import pandas as pd
import numpy as np

from src.shared.config.settings import settings
from src.domain.entities.weather import WeatherData, WeatherRecord


logger = logging.getLogger(__name__)


class DataCleaningService:
    """
    Service for cleaning and validating weather data.
    Implements the business logic for data quality assurance.
    """

    def __init__(self,
                 min_temp: float = settings.MIN_VALID_TEMPERATURE,
                 max_temp: float = settings.MAX_VALID_TEMPERATURE,
                 min_humidity: int = settings.MIN_VALID_HUMIDITY,
                 max_humidity: int = settings.MAX_VALID_HUMIDITY,
                 min_wind: float = settings.MIN_VALID_WIND_SPEED,
                 max_wind: float = settings.MAX_VALID_WIND_SPEED):
        """
        Initialize data cleaning service.

        Args:
            min_temp: Minimum valid temperature
            max_temp: Maximum valid temperature
            min_humidity: Minimum valid humidity
            max_humidity: Maximum valid humidity
            min_wind: Minimum valid wind speed
            max_wind: Maximum valid wind speed
        """
        self.min_temp = min_temp
        self.max_temp = max_temp
        self.min_humidity = min_humidity
        self.max_humidity = max_humidity
        self.min_wind = min_wind
        self.max_wind = max_wind

        logger.info("DataCleaningService initialized with validation thresholds")

    def clean_weather_data(self, 
                          weather_list: List[WeatherData]) -> Tuple[List[WeatherData], int]:
        """
        Clean and validate a list of weather data records.

        Args:
            weather_list: List of WeatherData entities

        Returns:
            Tuple of (cleaned_list, removed_count)
        """
        original_count = len(weather_list)
        cleaned_list = []
        removed_count = 0

        for weather in weather_list:
            if self._validate_record(weather):
                cleaned_weather = self._clean_record(weather)
                cleaned_list.append(cleaned_weather)
            else:
                removed_count += 1
                logger.warning(f"Removed invalid record for city: {weather.city}")

        logger.info(f"Cleaning completed: {original_count} → {len(cleaned_list)} records "
                   f"({removed_count} removed)")
        return cleaned_list, removed_count

    def _validate_record(self, weather: WeatherData) -> bool:
        """
        Validate a single weather record.

        Args:
            weather: WeatherData entity

        Returns:
            True if record is valid, False otherwise
        """
        # Check for null values
        if not weather.city or not weather.timestamp:
            return False

        # Validate temperature
        if weather.temperature < self.min_temp or weather.temperature > self.max_temp:
            logger.warning(f"Invalid temperature for {weather.city}: {weather.temperature}")
            return False

        # Validate humidity
        if weather.humidity < self.min_humidity or weather.humidity > self.max_humidity:
            logger.warning(f"Invalid humidity for {weather.city}: {weather.humidity}")
            return False

        # Validate wind speed
        if weather.wind_speed < self.min_wind or weather.wind_speed > self.max_wind:
            logger.warning(f"Invalid wind speed for {weather.city}: {weather.wind_speed}")
            return False

        return True

    def _clean_record(self, weather: WeatherData) -> WeatherData:
        """
        Clean a single weather record.
        Normalizes city names and ensures consistent formatting.

        Args:
            weather: WeatherData entity

        Returns:
            Cleaned WeatherData entity
        """
        # Normalize city name: title case
        city_name = weather.city.strip().title()

        if city_name == 'Turan':
            city_name = 'Da Nang'

        # Round numeric values to reasonable precision
        temperature = round(weather.temperature, 2)
        wind_speed = round(weather.wind_speed, 2)

        return WeatherData(
            city=city_name,
            timestamp=weather.timestamp,
            temperature=temperature,
            humidity=weather.humidity,
            wind_speed=wind_speed,
            description=weather.description.lower() if weather.description else None,
            pressure=weather.pressure,
            clouds=weather.clouds
        )

    def convert_to_dataframe(self, weather_list: List[WeatherData]) -> pd.DataFrame:
        """
        Convert list of WeatherData entities to pandas DataFrame.

        Args:
            weather_list: List of WeatherData entities

        Returns:
            pandas DataFrame
        """
        if not weather_list:
            logger.error("No weather data available to convert to DataFrame.")
            return pd.DataFrame(columns=['city', 'timestamp', 'temperature', 'humidity', 'wind_speed', 'description', 'pressure', 'clouds'])
        
        data = [w.to_dict() for w in weather_list]
        df = pd.DataFrame(data)

        # Ensure correct data types
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['temperature'] = df['temperature'].astype(float)
        df['humidity'] = df['humidity'].astype(int)
        df['wind_speed'] = df['wind_speed'].astype(float)

        logger.info(f"Converted {len(weather_list)} records to DataFrame")
        return df

    def remove_duplicates(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
        """
        Remove duplicate records from DataFrame.

        Args:
            df: Input DataFrame

        Returns:
            Tuple of (df_deduplicated, duplicates_count)
        """
        original_count = len(df)
        df_dedup = df.drop_duplicates(subset=['city', 'timestamp'])
        duplicates_count = original_count - len(df_dedup)

        if duplicates_count > 0:
            logger.info(f"Removed {duplicates_count} duplicate records")

        return df_dedup, duplicates_count

    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing values in DataFrame.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with missing values handled
        """
        # Log missing values
        missing = df.isnull().sum()
        if missing.sum() > 0:
            logger.info(f"Found missing values:\n{missing[missing > 0]}")

        # Drop rows with missing critical values
        df = df.dropna(subset=['city', 'timestamp', 'temperature', 'humidity', 'wind_speed'])

        # Fill missing non-critical columns with appropriate values
        df['description'] = df['description'].fillna('unknown')
        df['pressure'] = df['pressure'].fillna(0)
        df['clouds'] = df['clouds'].fillna(0)

        logger.info(f"Handled missing values, final record count: {len(df)}")
        return df

    def standardize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize DataFrame schema and column types.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with standardized schema
        """
        # Define expected schema
        expected_columns = [
            'city', 'timestamp', 'temperature', 'humidity', 
            'wind_speed', 'description', 'pressure', 'clouds'
        ]

        # Select only expected columns
        df = df[expected_columns]

        # Ensure correct data types
        df = df.astype({
            'city': 'object',
            'timestamp': 'datetime64[ns]',
            'temperature': 'float64',
            'humidity': 'int64',
            'wind_speed': 'float64',
            'description': 'object',
            'pressure': 'int64',
            'clouds': 'int64'
        })

        # Sort by city and timestamp
        df = df.sort_values(['city', 'timestamp'])

        logger.info("Schema standardized")
        return df
