"""
Configuration settings for Weather Data Pipeline
"""
import os
from typing import List
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(dotenv_path='/opt/airflow/.env')


class Settings:
    """
    Centralized configuration management for the entire application.
    All configuration values are loaded from environment variables.
    """

    # Airflow Configuration
    AIRFLOW_IMAGE_NAME: str = os.getenv('AIRFLOW_IMAGE_NAME', 'apache/airflow:2.10.2')
    AIRFLOW_UID: int = int(os.getenv('AIRFLOW_UID', 50000))

    # OpenWeather API Configuration
    OPENWEATHER_API_KEY: str = os.getenv('OPENWEATHER_API_KEY', '')
    OPENWEATHER_BASE_URL: str = 'http://api.openweathermap.org/data/2.5/weather'

    # City Configuration
    CITIES: List[str] = os.getenv("CITIES", "").split(";") if os.getenv("CITIES") else []

    # PostgreSQL Configuration
    POSTGRES_USER: str = os.getenv('POSTGRES_USER', 'airflow')
    POSTGRES_PASSWORD: str = os.getenv('POSTGRES_PASSWORD', 'airflow')
    POSTGRES_HOST: str = os.getenv('POSTGRES_HOST', 'postgres')
    POSTGRES_PORT: int = int(os.getenv('POSTGRES_PORT', 5432))
    POSTGRES_DB: str = os.getenv('POSTGRES_DB', 'weather_db')

    # PostgreSQL Connection String
    @property
    def POSTGRES_CONN(self) -> str:
        return (f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
                f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}")

    # PostgreSQL SQLAlchemy Connection String
    @property
    def POSTGRES_SQLALCHEMY_CONN(self) -> str:
        return (f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
                f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}")

    # MinIO Configuration
    MINIO_ENDPOINT: str = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
    MINIO_ACCESS_KEY: str = os.getenv('MINIO_ACCESS_KEY', 'ROOT_USER')
    MINIO_SECRET_KEY: str = os.getenv('MINIO_SECRET_KEY', 'CHANGEME123')
    MINIO_BUCKET: str = os.getenv('MINIO_BUCKET', 'weather-data')

    # MinIO Layer Paths
    MINIO_BRONZE_PREFIX: str = 'bronze'  # Raw JSON data
    MINIO_SILVER_PREFIX: str = 'silver'  # Cleaned Parquet data
    MINIO_GOLD_PREFIX: str = 'gold'      # Transformed data for warehouse

    # Logging Configuration
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FORMAT: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Data Processing Configuration
    REQUEST_TIMEOUT: int = int(os.getenv('REQUEST_TIMEOUT', 10))
    MAX_RETRIES: int = int(os.getenv('MAX_RETRIES', 2))
    RETRY_DELAY_SECONDS: int = int(os.getenv('RETRY_DELAY_SECONDS', 300))

    # Data Validation Configuration
    MIN_VALID_TEMPERATURE: float = -60.0  # Celsius
    MAX_VALID_TEMPERATURE: float = 60.0   # Celsius
    MIN_VALID_HUMIDITY: int = 0           # Percentage
    MAX_VALID_HUMIDITY: int = 100         # Percentage
    MIN_VALID_WIND_SPEED: float = 0.0      # m/s
    MAX_VALID_WIND_SPEED: float = 150.0   # m/s

    @classmethod
    def validate(cls) -> None:
        """Validate critical configuration settings."""
        if not cls.OPENWEATHER_API_KEY:
            raise ValueError("OPENWEATHER_API_KEY environment variable is not set")
        if not cls.CITIES:
            raise ValueError("CITIES environment variable is not set")


# Create a singleton instance of settings
settings = Settings()
