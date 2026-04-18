import os
from typing import List

from dotenv import load_dotenv


load_dotenv(dotenv_path='/opt/airflow/.env')


class Settings:
    """Centralized configuration for the application."""

    AIRFLOW_IMAGE_NAME: str = os.getenv('AIRFLOW_IMAGE_NAME', 'apache/airflow:2.10.2')
    AIRFLOW_UID: int = int(os.getenv('AIRFLOW_UID', 50000))

    OPENWEATHER_API_KEY: str = os.getenv('OPENWEATHER_API_KEY', '')
    OPENWEATHER_BASE_URL: str = 'http://api.openweathermap.org/data/2.5/weather'

    CITIES: List[str] = os.getenv("CITIES", "").split(";") if os.getenv("CITIES") else []

    POSTGRES_USER: str = os.getenv('POSTGRES_USER', 'airflow')
    POSTGRES_PASSWORD: str = os.getenv('POSTGRES_PASSWORD', 'airflow')
    POSTGRES_HOST: str = os.getenv('POSTGRES_HOST', 'postgres')
    POSTGRES_PORT: int = int(os.getenv('POSTGRES_PORT', 5432))
    POSTGRES_DB: str = os.getenv('POSTGRES_DB', 'weather_db')

    @property
    def POSTGRES_CONN(self) -> str:
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def POSTGRES_SQLALCHEMY_CONN(self) -> str:
        return (
            f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    MINIO_ENDPOINT: str = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
    MINIO_ACCESS_KEY: str = os.getenv('MINIO_ACCESS_KEY', 'ROOT_USER')
    MINIO_SECRET_KEY: str = os.getenv('MINIO_SECRET_KEY', 'CHANGEME123')
    MINIO_BUCKET: str = os.getenv('MINIO_BUCKET', 'weather-data')
    MINIO_RAW_PREFIX: str = os.getenv('MINIO_RAW_PREFIX', 'raw_data')
    MINIO_CLEAN_PREFIX: str = os.getenv('MINIO_CLEAN_PREFIX', 'clean_data')
    MINIO_GOLD_PREFIX: str = os.getenv('MINIO_GOLD_PREFIX', 'gold')

    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FORMAT: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    REQUEST_TIMEOUT: int = int(os.getenv('REQUEST_TIMEOUT', 10))
    MAX_RETRIES: int = int(os.getenv('MAX_RETRIES', 2))
    RETRY_DELAY_SECONDS: int = int(os.getenv('RETRY_DELAY_SECONDS', 300))

    MIN_VALID_TEMPERATURE: float = -60.0
    MAX_VALID_TEMPERATURE: float = 60.0
    MIN_VALID_HUMIDITY: int = 0
    MAX_VALID_HUMIDITY: int = 100
    MIN_VALID_WIND_SPEED: float = 0.0
    MAX_VALID_WIND_SPEED: float = 150.0

    @classmethod
    def validate(cls) -> None:
        if not cls.OPENWEATHER_API_KEY:
            raise ValueError("OPENWEATHER_API_KEY environment variable is not set")
        if not cls.CITIES:
            raise ValueError("CITIES environment variable is not set")


settings = Settings()
