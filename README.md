# Weather Data Pipeline

An end-to-end weather data pipeline for provinces and cities in Vietnam, using Airflow for orchestration, MinIO as the data lake, PostgreSQL as the staging warehouse, dbt for transformations, and Power BI for visualization.

## Architecture

![Architecture Diagram](images/architecture.png)

Current processing flow:

1. `fetch_weather`: calls OpenWeather by province/city coordinates and stores the raw payload in `s3://weather-data/raw_data/`
2. `clean_weather_data`: cleans and standardizes the data, then stores parquet files in `s3://weather-data/clean_data/`
3. `save_to_postgres`: loads the cleaned data into the `weather_data` staging table
4. `trigger_dbt`: runs the analytics transformation layer to build `dim_city` and `fact_weather`

## Tech Stack

| Component | Technology | Role |
|-----------|-----------|------|
| Orchestration | Apache Airflow 2.10.2 | DAG orchestration |
| Data Storage | MinIO | Stores `raw_data` and `clean_data` |
| Warehouse | PostgreSQL 15 | Stores staging and data mart tables |
| Transformation | dbt 1.5 | Builds transformation models |
| Visualization | Power BI | Dashboards and visualization |
| Programming Language | Python 3.11 | Pipeline processing |

## Project Structure

```text
Weather_Pipeline/
├── dags/
│   └── weather_pipeline_dag.py
├── dbt/
│   ├── macros/
│   ├── models/
│   │   ├── marts/
│   │   └── staging/
│   ├── tests/
│   ├── dbt_project.yml
│   └── profiles.yml
├── docker/
│   └── superset/
│       └── Dockerfile
├── images/
├── src/
│   ├── application/
│   ├── domain/
│   ├── infrastructure/
│   ├── interfaces/
│   └── shared/
├── tests/
├── .env.example
├── docker-compose.yaml
├── requirements.txt
└── setup.sh
```

## Main Configuration

Important variables in `.env`:

```env
OPENWEATHER_API_KEY=your_api_key
CITIES=Ha_Noi;Ho_Chi_Minh;Da_Nang;...

POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=weather_db
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow

MINIO_BUCKET=weather-data
MINIO_RAW_PREFIX=raw_data
MINIO_CLEAN_PREFIX=clean_data
MINIO_GOLD_PREFIX=gold
```

## Run the Project

- On the first run, it is recommended to initialize `airflow-init` first so the database and bucket are created. On later runs, `docker compose up -d` is usually enough.

```bash
docker compose up -d
docker compose run --rm airflow-init
```

Services:

- Airflow: `http://localhost:8080`
- MinIO Console: `http://localhost:9001`
- PostgreSQL from host: `localhost:5432`

## PostgreSQL Connection

From the host machine:

```text
Host: localhost
Port: 5432
Database: weather_db
Username: airflow
Password: airflow
```

From internal containers:

```text
postgresql+psycopg2://airflow:airflow@postgres:5432/weather_db
```

## MinIO Layout

```text
weather-data/
├── raw_data/
└── clean_data/
```

## dbt Models

- `stg_weather_data`: staging view built from `weather_data`
- `dim_city`: city/province dimension table
- `fact_weather`: weather fact table

## Useful Commands

```bash
docker compose logs airflow-scheduler
docker compose logs postgres
docker compose exec postgres psql -U airflow -d weather_db -c "SELECT COUNT(*) FROM weather_data;"
docker compose exec minio mc ls minio/weather-data/raw_data
docker compose exec minio mc ls minio/weather-data/clean_data
docker compose exec dbt dbt run --profiles-dir /root/.dbt
```

## Tests

```bash
docker compose run --rm airflow pytest tests/
```

## Power BI Dashboard

### Temperature Dashboard

![Temperature Dashboard](images/image.png)

### Humidity, Wind Speed, and Pressure Dashboard

![Humidity, wind speed, pressure dashboard](images/image-4.png)

### Weather State in Each Province

![Weather State in each province](images/image-2.png)

### Heatmap in Vietnam

![Heatmap in Vietnam](images/image-3.png)

## Notes

- If port `5432` is already in use, you can switch to `5433` and update `.env` and `docker-compose.yaml`, or stop the conflicting `postgres` service and run again.
- Make sure `mc` is installed in the MinIO container if you want to inspect stored files.
- `dbt/profiles.yml` must be configured correctly to connect to PostgreSQL.
