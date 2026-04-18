import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


sys.path.insert(0, str(Path(__file__).parent.parent))

from src.interfaces.airflow.operators import PipelineOperations


default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}


with DAG(
    dag_id='weather_data_pipeline',
    default_args=default_args,
    description='End-to-end weather data pipeline: raw data, cleaning, warehouse',
    schedule_interval='0 */3 * * *',
    catchup=False,
    tags=['weather', 'production', 'data-pipeline'],
) as dag:
    fetch_weather_task = PythonOperator(
        task_id='fetch_weather',
        python_callable=PipelineOperations.fetch_weather_data,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    clean_weather_data_task = PythonOperator(
        task_id='clean_weather_data',
        python_callable=PipelineOperations.clean_weather_data,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    save_to_postgres_task = PythonOperator(
        task_id='save_to_postgres',
        python_callable=PipelineOperations.save_to_postgres,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    trigger_dbt_task = PythonOperator(
        task_id='trigger_dbt',
        python_callable=PipelineOperations.trigger_dbt,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    fetch_weather_task >> clean_weather_data_task >> save_to_postgres_task >> trigger_dbt_task
