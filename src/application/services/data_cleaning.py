import logging
from typing import List, Tuple

import pandas as pd

from src.domain.entities.weather import WeatherData
from src.shared.config.settings import settings


logger = logging.getLogger(__name__)


class DataCleaningService:
    """Business logic for validating and normalizing weather data."""

    def __init__(
        self,
        min_temp: float = settings.MIN_VALID_TEMPERATURE,
        max_temp: float = settings.MAX_VALID_TEMPERATURE,
        min_humidity: int = settings.MIN_VALID_HUMIDITY,
        max_humidity: int = settings.MAX_VALID_HUMIDITY,
        min_wind: float = settings.MIN_VALID_WIND_SPEED,
        max_wind: float = settings.MAX_VALID_WIND_SPEED,
    ):
        self.min_temp = min_temp
        self.max_temp = max_temp
        self.min_humidity = min_humidity
        self.max_humidity = max_humidity
        self.min_wind = min_wind
        self.max_wind = max_wind

        logger.info("DataCleaningService initialized with validation thresholds")

    def clean_weather_data(self, weather_list: List[WeatherData]) -> Tuple[List[WeatherData], int]:
        """Validate and normalize weather records."""
        original_count = len(weather_list)
        cleaned_list: List[WeatherData] = []
        removed_count = 0

        for weather in weather_list:
            if self._validate_record(weather):
                cleaned_list.append(self._clean_record(weather))
            else:
                removed_count += 1
                logger.warning(f"Removed invalid record for city: {weather.city}")

        logger.info(
            f"Cleaning completed: {original_count} -> {len(cleaned_list)} records "
            f"({removed_count} removed)"
        )
        return cleaned_list, removed_count

    def _validate_record(self, weather: WeatherData) -> bool:
        if not weather.city or not weather.timestamp:
            return False

        if not self.min_temp <= weather.temperature <= self.max_temp:
            logger.warning(f"Invalid temperature for {weather.city}: {weather.temperature}")
            return False

        if not self.min_humidity <= weather.humidity <= self.max_humidity:
            logger.warning(f"Invalid humidity for {weather.city}: {weather.humidity}")
            return False

        if not self.min_wind <= weather.wind_speed <= self.max_wind:
            logger.warning(f"Invalid wind speed for {weather.city}: {weather.wind_speed}")
            return False

        return True

    def _clean_record(self, weather: WeatherData) -> WeatherData:
        city_name = weather.city.strip().title()
        if city_name == 'Turan':
            city_name = 'Da Nang'

        return WeatherData(
            city=city_name,
            timestamp=weather.timestamp,
            temperature=round(weather.temperature, 2),
            humidity=weather.humidity,
            wind_speed=round(weather.wind_speed, 2),
            description=weather.description.lower() if weather.description else None,
            pressure=weather.pressure,
            clouds=weather.clouds,
        )

    def convert_to_dataframe(self, weather_list: List[WeatherData]) -> pd.DataFrame:
        if not weather_list:
            logger.error("No weather data available to convert to DataFrame")
            return pd.DataFrame(
                columns=[
                    'city',
                    'timestamp',
                    'temperature',
                    'humidity',
                    'wind_speed',
                    'description',
                    'pressure',
                    'clouds',
                ]
            )

        df = pd.DataFrame([weather.to_dict() for weather in weather_list])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['temperature'] = df['temperature'].astype(float)
        df['humidity'] = df['humidity'].astype(int)
        df['wind_speed'] = df['wind_speed'].astype(float)

        logger.info(f"Converted {len(weather_list)} records to DataFrame")
        return df

    def remove_duplicates(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
        original_count = len(df)
        df_dedup = df.drop_duplicates(subset=['city', 'timestamp'])
        duplicates_count = original_count - len(df_dedup)

        if duplicates_count > 0:
            logger.info(f"Removed {duplicates_count} duplicate records")

        return df_dedup, duplicates_count

    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = df.isnull().sum()
        if missing.sum() > 0:
            logger.info(f"Found missing values:\n{missing[missing > 0]}")

        df = df.dropna(subset=['city', 'timestamp', 'temperature', 'humidity', 'wind_speed'])
        df['description'] = df['description'].fillna('unknown')
        df['pressure'] = df['pressure'].fillna(0)
        df['clouds'] = df['clouds'].fillna(0)

        logger.info(f"Handled missing values, final record count: {len(df)}")
        return df

    def standardize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        expected_columns = [
            'city',
            'timestamp',
            'temperature',
            'humidity',
            'wind_speed',
            'description',
            'pressure',
            'clouds',
        ]

        df = df[expected_columns]
        df = df.astype(
            {
                'city': 'object',
                'timestamp': 'datetime64[ns]',
                'temperature': 'float64',
                'humidity': 'int64',
                'wind_speed': 'float64',
                'description': 'object',
                'pressure': 'int64',
                'clouds': 'int64',
            }
        )
        df = df.sort_values(['city', 'timestamp'])

        logger.info("Schema standardized")
        return df
