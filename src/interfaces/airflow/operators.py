"""
Airflow operator functions for the weather data pipeline.
These functions are called by Airflow tasks and delegate to application services.
"""
import logging
from typing import Optional

from src.application.services.orchestration import WeatherPipelineOrchestrator
from src.shared.config.settings import settings
from src.shared.logging.logger import get_logger


logger = get_logger(__name__)


class PipelineOperations:
    """
    Encapsulates pipeline operations for use with Airflow.
    Each method represents a logical task in the DAG.
    """

    @staticmethod
    def fetch_weather_data(**context) -> Optional[str]:
        """
        Airflow task: Fetch weather data from API and save to Bronze layer.

        Args:
            context: Airflow context (XCom communication)

        Returns:
            Path to saved raw data in MinIO
        """
        logger.info("Task: Fetch weather data from OpenWeather API")

        try:
            orchestrator = WeatherPipelineOrchestrator()
            bronze_path = orchestrator.fetch_and_save_bronze(settings.CITIES)

            if not bronze_path:
                raise Exception("Failed to fetch or save bronze layer data")

            # Push path to XCom for next task
            context['ti'].xcom_push(key='bronze_path', value=bronze_path)

            logger.info(f"Fetch task completed. Bronze path: {bronze_path}")
            return bronze_path

        except Exception as e:
            logger.error(f"Fetch task failed: {e}")
            raise

    @staticmethod
    def transform_to_silver(**context) -> Optional[str]:
        """
        Airflow task: Transform Bronze data to Silver layer (clean and validate).

        Args:
            context: Airflow context (XCom communication)

        Returns:
            Path to cleaned data in MinIO
        """
        logger.info("Task: Transform data to Silver layer")

        try:
            # Get bronze path from previous task
            bronze_path = context['ti'].xcom_pull(
                task_ids='fetch_weather',
                key='bronze_path'
            )

            if not bronze_path:
                raise Exception("Bronze path not found in XCom")

            orchestrator = WeatherPipelineOrchestrator()
            silver_path = orchestrator.transform_to_silver(bronze_path)

            if not silver_path:
                raise Exception("Failed to transform to silver layer")

            # Push path to XCom for next task
            context['ti'].xcom_push(key='silver_path', value=silver_path)

            logger.info(f"Transform task completed. Silver path: {silver_path}")
            return silver_path

        except Exception as e:
            logger.error(f"Transform task failed: {e}")
            raise

    @staticmethod
    def save_to_postgres(**context) -> bool:
        """
        Airflow task: Save cleaned data to PostgreSQL staging table.

        Args:
            context: Airflow context (XCom communication)

        Returns:
            True if successful
        """
        logger.info("Task: Save data to PostgreSQL")

        try:
            # Get silver path from previous task
            silver_path = context['ti'].xcom_pull(
                task_ids='transform_to_silver',
                key='silver_path'
            )

            if not silver_path:
                raise Exception("Silver path not found in XCom")

            orchestrator = WeatherPipelineOrchestrator()
            success = orchestrator.save_to_postgres_staging(silver_path)

            if not success:
                raise Exception("Failed to save to PostgreSQL")

            logger.info("Save task completed")
            return success

        except Exception as e:
            logger.error(f"Save task failed: {e}")
            raise

    @staticmethod
    def trigger_dbt(**context) -> bool:
        """
        Airflow task: Trigger dbt to build Gold layer models.

        Args:
            context: Airflow context

        Returns:
            True if successful
        """
        logger.info("Task: Trigger dbt models")

        try:
            orchestrator = WeatherPipelineOrchestrator()
            success = orchestrator.trigger_dbt_models()

            if not success:
                raise Exception("Failed to trigger dbt")

            logger.info("dbt trigger task completed")
            return success

        except Exception as e:
            logger.error(f"dbt trigger task failed: {e}")
            raise
