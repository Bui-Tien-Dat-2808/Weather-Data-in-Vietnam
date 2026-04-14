"""
Orchestration service that coordinates the entire data pipeline.
Handles the workflow of fetching, cleaning, and storing data.
"""
import json
import logging
import tempfile
from pathlib import Path
from typing import List, Optional, Tuple
from datetime import datetime

import pandas as pd

from src.shared.config.settings import settings
from src.infrastructure.api.openweather import OpenWeatherAPIClient
from src.infrastructure.storage.minio import MinIOConnector
from src.infrastructure.storage.postgres import PostgreSQLConnector
from src.application.services.data_cleaning import DataCleaningService
from src.domain.entities.weather import WeatherData


logger = logging.getLogger(__name__)


class WeatherPipelineOrchestrator:
    """
    Main orchestration service for the weather data pipeline.
    Coordinates all stages: fetch → clean → store.
    """

    def __init__(self):
        """Initialize pipeline orchestrator with all dependencies."""
        self.api_client = OpenWeatherAPIClient()
        self.minio = MinIOConnector()
        self.postgres = PostgreSQLConnector()
        self.cleaning_service = DataCleaningService()

        logger.info("WeatherPipelineOrchestrator initialized")

    def fetch_and_save_bronze(self, cities: List[str]) -> Optional[str]:
        """
        Fetch weather data from API and save to MinIO Bronze layer.

        Args:
            cities: List of city names to fetch

        Returns:
            Path to the saved raw data file
        """
        logger.info(f"Starting Bronze layer: fetching data for {len(cities)} cities")

        # Fetch data from API
        api_responses = self.api_client.fetch_multiple_cities(cities)

        if not api_responses:
            logger.error("Failed to fetch any weather data")
            return None

        # Transform to WeatherData entities
        weather_list: List[WeatherData] = []
        for response in api_responses:
            entity = self.api_client.transform_to_entity(response)
            if entity:
                weather_list.append(entity)

        logger.info(f"Transformed {len(weather_list)} API responses to entities")

        # Save raw data as JSON locally
        timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        local_path = f"/tmp/weather_raw_{timestamp}.json"

        raw_data = [w.to_dict() for w in weather_list]
        try:
            with open(local_path, 'w') as f:
                json.dump(raw_data, f, indent=2, default=str)
            logger.info(f"Saved raw data to {local_path}")
        except IOError as e:
            logger.error(f"Failed to save raw data: {e}")
            return None

        # Upload to MinIO Bronze layer
        object_name = f"{settings.MINIO_BRONZE_PREFIX}/weather_raw_{timestamp}.json"
        success = self.minio.upload_file(local_path, object_name)

        if not success:
            logger.error("Failed to upload raw data to MinIO")
            return None

        # Clean up local file
        try:
            Path(local_path).unlink()
        except:
            pass

        logger.info(f"Bronze layer completed: {len(weather_list)} records saved")
        return object_name

    def transform_to_silver(self, bronze_path: str) -> Optional[str]:
        """
        Download data from Bronze layer, clean it, and save to Silver layer.

        Args:
            bronze_path: Path to raw data in MinIO

        Returns:
            Path to the cleaned data file in MinIO
        """
        logger.info(f"Starting Silver layer: transforming {bronze_path}")

        # Download raw data from MinIO
        timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        local_raw_path = f"/tmp/weather_raw_{timestamp}.json"

        success = self.minio.download_file(bronze_path, local_raw_path)
        if not success:
            logger.error(f"Failed to download {bronze_path}")
            return None

        # Load raw data
        try:
            with open(local_raw_path, 'r') as f:
                raw_data = json.load(f)
        except IOError as e:
            logger.error(f"Failed to read raw data: {e}")
            return None

        # Transform to entities and clean
        weather_list = [
            self.api_client.transform_to_entity(record) 
            for record in raw_data
        ]
        weather_list = [w for w in weather_list if w is not None]

        cleaned_list, removed = self.cleaning_service.clean_weather_data(weather_list)

        # Convert to DataFrame
        df = self.cleaning_service.convert_to_dataframe(cleaned_list)
        df, duplicates = self.cleaning_service.remove_duplicates(df)
        df = self.cleaning_service.handle_missing_values(df)
        df = self.cleaning_service.standardize_schema(df)

        # Upload as Parquet to Silver layer
        silver_filename = f"weather_silver_{timestamp}.parquet"
        silver_path = f"{settings.MINIO_SILVER_PREFIX}/{silver_filename}"

        success = self.minio.upload_dataframe_as_parquet(df, silver_path)

        if not success:
            logger.error("Failed to upload data to Silver layer")
            return None

        # Clean up local file
        try:
            Path(local_raw_path).unlink()
        except:
            pass

        logger.info(f"Silver layer completed: {len(df)} cleaned records saved")
        return silver_path

    def save_to_postgres_staging(self, silver_path: str) -> bool:
        """
        Download cleaned data from Silver layer and save to PostgreSQL staging table.

        Args:
            silver_path: Path to cleaned data in MinIO

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Starting PostgreSQL staging: loading {silver_path}")

        # Download Parquet from MinIO
        df = self.minio.download_parquet_as_dataframe(silver_path)
        if df is None:
            logger.error(f"Failed to download {silver_path}")
            return False

        # Ensure weather_data table exists
        if not self.postgres.table_exists('weather_data'):
            success = self.postgres.create_weather_data_table()
            if not success:
                logger.error("Failed to create weather_data table")
                return False

        # Insert into PostgreSQL
        success = self.postgres.insert_dataframe(df, 'weather_data')

        if success:
            logger.info(f"PostgreSQL staging completed: {len(df)} records inserted")

        return success

    def trigger_dbt_models(self) -> bool:
        """
        Trigger dbt to build gold layer models from staging data.
        This is a placeholder - actual implementation depends on dbt setup.

        Returns:
            True if successful, False otherwise
        """
        logger.info("Triggering dbt models for gold layer transformation")
        
        # Note: In the actual implementation, this would call dbt CLI or API
        # For now, we'll just log it as a task that Airflow will handle
        logger.info("dbt trigger delegated to Airflow task")
        
        return True

    def run_full_pipeline(self, cities: Optional[List[str]] = None) -> bool:
        """
        Run the complete pipeline: Bronze → Silver → Staging → dbt.

        Args:
            cities: List of cities to fetch (uses settings.CITIES if not provided)

        Returns:
            True if pipeline completed successfully
        """
        cities = cities or settings.CITIES

        logger.info("Starting full weather data pipeline")

        try:
            # Bronze: Fetch and save raw data
            bronze_path = self.fetch_and_save_bronze(cities)
            if not bronze_path:
                logger.error("Bronze layer failed")
                return False

            # Silver: Clean and transform data
            silver_path = self.transform_to_silver(bronze_path)
            if not silver_path:
                logger.error("Silver layer failed")
                return False

            # Save to PostgreSQL staging
            if not self.save_to_postgres_staging(silver_path):
                logger.error("PostgreSQL staging failed")
                return False

            logger.info("Full pipeline completed successfully")
            return True

        except Exception as e:
            logger.error(f"Pipeline failed with error: {e}")
            return False

    def cleanup(self):
        """Clean up resources."""
        self.postgres.disconnect()
        logger.info("Pipeline cleanup completed")
