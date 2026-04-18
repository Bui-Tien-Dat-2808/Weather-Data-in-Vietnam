import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from src.application.services.data_cleaning import DataCleaningService
from src.infrastructure.api.openweather import OpenWeatherAPIClient
from src.infrastructure.storage.minio import MinIOConnector
from src.infrastructure.storage.postgres import PostgreSQLConnector
from src.shared.config.settings import settings


logger = logging.getLogger(__name__)


class WeatherPipelineOrchestrator:
    """Coordinate data fetching, cleaning, and persistence."""

    def __init__(self):
        self.api_client = OpenWeatherAPIClient()
        self.minio = MinIOConnector()
        self.postgres = PostgreSQLConnector()
        self.cleaning_service = DataCleaningService()

        logger.info("WeatherPipelineOrchestrator initialized")

    def fetch_and_save_raw_data(self, cities: List[str]) -> Optional[str]:
        """Fetch weather data and persist the raw API payloads to MinIO."""
        logger.info(f"Fetching weather data for {len(cities)} cities")

        api_responses = self.api_client.fetch_multiple_cities(cities)
        if not api_responses:
            logger.error("Failed to fetch any weather data")
            return None

        timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        local_path = f"/tmp/weather_raw_{timestamp}.json"

        try:
            with open(local_path, 'w', encoding='utf-8') as file:
                json.dump(api_responses, file, indent=2, default=str)
        except OSError as exc:
            logger.error(f"Failed to save raw data locally: {exc}")
            return None

        raw_data_path = f"{settings.MINIO_RAW_PREFIX}/weather_raw_{timestamp}.json"
        success = self.minio.upload_file(local_path, raw_data_path)

        try:
            Path(local_path).unlink(missing_ok=True)
        except OSError:
            pass

        if not success:
            logger.error("Failed to upload raw data to MinIO")
            return None

        logger.info(f"Raw data stage completed: {len(api_responses)} records saved")
        return raw_data_path

    def clean_weather_data(self, raw_data_path: str) -> Optional[str]:
        """Load raw data from MinIO, clean it, and save parquet output back to MinIO."""
        logger.info(f"Cleaning weather data from {raw_data_path}")

        timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        local_raw_path = f"/tmp/weather_raw_{timestamp}.json"

        success = self.minio.download_file(raw_data_path, local_raw_path)
        if not success:
            logger.error(f"Failed to download {raw_data_path}")
            return None

        try:
            with open(local_raw_path, 'r', encoding='utf-8') as file:
                raw_data = json.load(file)
        except OSError as exc:
            logger.error(f"Failed to read raw data: {exc}")
            return None
        finally:
            try:
                Path(local_raw_path).unlink(missing_ok=True)
            except OSError:
                pass

        weather_list = [
            weather
            for weather in (
                self.api_client.transform_to_entity(record)
                for record in raw_data
            )
            if weather is not None
        ]

        cleaned_list, removed_count = self.cleaning_service.clean_weather_data(weather_list)
        logger.info(f"Cleaning service removed {removed_count} invalid records")

        df = self.cleaning_service.convert_to_dataframe(cleaned_list)
        if df.empty:
            logger.warning("No valid data available after cleaning")
            return None

        df, duplicate_count = self.cleaning_service.remove_duplicates(df)
        if duplicate_count:
            logger.info(f"Removed {duplicate_count} duplicate records")

        df = self.cleaning_service.handle_missing_values(df)
        df = self.cleaning_service.standardize_schema(df)

        clean_data_path = f"{settings.MINIO_CLEAN_PREFIX}/weather_clean_{timestamp}.parquet"
        success = self.minio.upload_dataframe_as_parquet(df, clean_data_path)

        if not success:
            logger.error("Failed to upload clean data to MinIO")
            return None

        logger.info(f"Clean data stage completed: {len(df)} records saved")
        return clean_data_path

    def save_to_postgres_staging(self, clean_data_path: str) -> bool:
        """Load clean parquet data from MinIO and append it to PostgreSQL staging."""
        logger.info(f"Loading clean data into PostgreSQL from {clean_data_path}")

        df = self.minio.download_parquet_as_dataframe(clean_data_path)
        if df is None:
            logger.error(f"Failed to download {clean_data_path}")
            return False

        if not self.postgres.table_exists('weather_data'):
            success = self.postgres.create_weather_data_table()
            if not success:
                logger.error("Failed to create weather_data table")
                return False

        success = self.postgres.insert_dataframe(df, 'weather_data')
        if success:
            logger.info(f"PostgreSQL staging completed: {len(df)} records inserted")

        return success

    def trigger_dbt_models(self) -> bool:
        """Placeholder for dbt orchestration."""
        logger.info("dbt execution is delegated to the Airflow/dbt integration")
        return True

    def run_full_pipeline(self, cities: Optional[List[str]] = None) -> bool:
        """Run the full pipeline end to end."""
        cities = cities or settings.CITIES
        logger.info("Starting full weather data pipeline")

        try:
            raw_data_path = self.fetch_and_save_raw_data(cities)
            if not raw_data_path:
                logger.error("Raw data stage failed")
                return False

            clean_data_path = self.clean_weather_data(raw_data_path)
            if not clean_data_path:
                logger.error("Clean data stage failed")
                return False

            if not self.save_to_postgres_staging(clean_data_path):
                logger.error("PostgreSQL staging failed")
                return False

            logger.info("Full pipeline completed successfully")
            return True
        except Exception as exc:
            logger.error(f"Pipeline failed with error: {exc}")
            return False

    def cleanup(self):
        self.postgres.disconnect()
        logger.info("Pipeline cleanup completed")
