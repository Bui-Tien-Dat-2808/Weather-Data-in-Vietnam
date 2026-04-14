"""
Refactored Airflow DAG for weather data pipeline.
Uses clean architecture with business logic separated from orchestration.
"""
import sys
from pathlib import Path

# Add parent directory to path to enable imports from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.interfaces.airflow.operators import PipelineOperations

# Default arguments for DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# DAG definition
with DAG(
    dag_id='weather_data_pipeline',
    default_args=default_args,
    description='End-to-End Weather Data Pipeline: Bronze → Silver → Gold',
    schedule_interval='0 */3 * * *',  # Every 3 hours
    catchup=False,
    tags=['weather', 'production', 'medallion-architecture'],
) as dag:

    # Task 1: Fetch weather data from API (Bronze Layer)
    fetch_weather_task = PythonOperator(
        task_id='fetch_weather',
        python_callable=PipelineOperations.fetch_weather_data,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    # Task 2: Transform to Silver layer (clean and validate)
    transform_to_silver_task = PythonOperator(
        task_id='transform_to_silver',
        python_callable=PipelineOperations.transform_to_silver,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    # Task 3: Save to PostgreSQL staging table
    save_to_postgres_task = PythonOperator(
        task_id='save_to_postgres',
        python_callable=PipelineOperations.save_to_postgres,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    # Task 4: Trigger dbt models (Gold Layer)
    trigger_dbt_task = PythonOperator(
        task_id='trigger_dbt',
        python_callable=PipelineOperations.trigger_dbt,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    # Define task dependencies
    fetch_weather_task >> transform_to_silver_task >> save_to_postgres_task >> trigger_dbt_task
