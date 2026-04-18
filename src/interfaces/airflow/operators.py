from typing import Optional

from src.application.services.orchestration import WeatherPipelineOrchestrator
from src.shared.config.settings import settings
from src.shared.logging.logger import get_logger


logger = get_logger(__name__)


class PipelineOperations:
    """Airflow task callables for the pipeline."""

    @staticmethod
    def fetch_weather_data(**context) -> Optional[str]:
        """Fetch weather data from the API and store raw payloads in MinIO."""
        logger.info("Task: fetch raw weather data")

        try:
            orchestrator = WeatherPipelineOrchestrator()
            raw_data_path = orchestrator.fetch_and_save_raw_data(settings.CITIES)

            if not raw_data_path:
                raise RuntimeError("Failed to fetch or save raw weather data")

            context['ti'].xcom_push(key='raw_data_path', value=raw_data_path)
            logger.info(f"Fetch task completed. Raw data path: {raw_data_path}")
            return raw_data_path
        except Exception as exc:
            logger.error(f"Fetch task failed: {exc}")
            raise

    @staticmethod
    def clean_weather_data(**context) -> Optional[str]:
        """Clean raw weather data and store the result in MinIO."""
        logger.info("Task: clean weather data")

        try:
            raw_data_path = context['ti'].xcom_pull(
                task_ids='fetch_weather',
                key='raw_data_path',
            )

            if not raw_data_path:
                raise RuntimeError("Raw data path not found in XCom")

            orchestrator = WeatherPipelineOrchestrator()
            clean_data_path = orchestrator.clean_weather_data(raw_data_path)

            if not clean_data_path:
                raise RuntimeError("Failed to clean weather data")

            context['ti'].xcom_push(key='clean_data_path', value=clean_data_path)
            logger.info(f"Clean task completed. Clean data path: {clean_data_path}")
            return clean_data_path
        except Exception as exc:
            logger.error(f"Clean task failed: {exc}")
            raise

    @staticmethod
    def save_to_postgres(**context) -> bool:
        """Save clean data from MinIO into PostgreSQL staging."""
        logger.info("Task: save clean data to PostgreSQL")

        try:
            clean_data_path = context['ti'].xcom_pull(
                task_ids='clean_weather_data',
                key='clean_data_path',
            )

            if not clean_data_path:
                raise RuntimeError("Clean data path not found in XCom")

            orchestrator = WeatherPipelineOrchestrator()
            success = orchestrator.save_to_postgres_staging(clean_data_path)

            if not success:
                raise RuntimeError("Failed to save to PostgreSQL")

            logger.info("Save task completed")
            return success
        except Exception as exc:
            logger.error(f"Save task failed: {exc}")
            raise

    @staticmethod
    def trigger_dbt(**context) -> bool:
        """Trigger dbt to build analytics models."""
        logger.info("Task: trigger dbt models")

        try:
            orchestrator = WeatherPipelineOrchestrator()
            success = orchestrator.trigger_dbt_models()

            if not success:
                raise RuntimeError("Failed to trigger dbt")

            logger.info("dbt trigger task completed")
            return success
        except Exception as exc:
            logger.error(f"dbt trigger task failed: {exc}")
            raise
