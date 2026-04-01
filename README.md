# Weather Data Pipeline with Airflow, MinIO, and PostgreSQL

## Overview

This project implements a data pipeline to collect weather data for all provinces in Vietnam using Apache Airflow. The pipeline runs every 3 hours, fetches weather data from the OpenWeather API, stores raw data in MinIO, and saves structured data into PostgreSQL for analysis.

## Tech Stack

* Apache Airflow 2.10.2 (Python 3.11)
* PostgreSQL (metadata and processed data storage)
* MinIO (object storage for raw JSON data)
* Docker Compose (container orchestration)
* DBeaver (database visualization)

## Project Structure

```
├── 📁 dags
├── 📁 logs
├── 📁 plugins
├── ⚙️ .gitignore
├── 📝 README.md
├── 🐍 Weather_Pipeline.py
├── ⚙️ docker-compose.yaml
└── 📄 requirements.txt
```

## Features

* Collect weather data (temperature, humidity, wind speed, etc.)
* Support all 63 provinces in Vietnam
* Run automatically every 3 hours
* Store raw data in MinIO
* Store processed data in PostgreSQL
* Retry mechanism for failed tasks

## Environment Variables

Create a `.env` file with the following content:

```
AIRFLOW_IMAGE_NAME=apache/airflow:2.10.2-python3.11
AIRFLOW_UID=50000

OPENWEATHER_API_KEY=your_api_key

POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_DB=your_postgres_db
POSTGRES_HOST=weather_db
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=ROOT_USER (default)
MINIO_SECRET_KEY=CHANGEME123(default)
MINIO_BUCKET=weather-data

CITIES="Hanoi,vn;Ho Chi Minh,vn;Da Nang,vn;..."
```

## Setup and Run

### 1. Start services

```bash
docker-compose up -d
```

### 2. Initialize Airflow (first time only)

```bash
docker-compose run --rm airflow-init
```

### 3. Access services

* Airflow Web UI: http://localhost:8080
  Username: airflow
  Password: airflow

* MinIO Console: http://localhost:9001
  Username: ROOT_USER
  Password: CHANGEME123

### 4. Create MinIO bucket

* Open MinIO Console
* Create a bucket named `weather-data`

## DAG Workflow

The pipeline consists of three tasks:

1. **fetch_weather**

   * Fetch weather data from OpenWeather API for all cities
   * Save raw data as JSON

2. **upload_to_minio**

   * Upload raw JSON file to MinIO bucket

3. **save_to_postgres**

   * Transform JSON into structured format
   * Insert data into PostgreSQL table `weather_data`

## Database Schema

Table: `weather_data`

| Column      | Type      |
| ----------- | --------- |
| city        | TEXT      |
| timestamp   | TIMESTAMP |
| temperature | FLOAT     |
| humidity    | INTEGER   |
| wind_speed  | FLOAT     |

## Query Example

```bash
SELECT * FROM weather_data LIMIT 10;
```

## Troubleshooting

### Airflow Web UI not accessible

* Check if `airflow-webserver` container is running:

```bash
docker ps
```

* Check logs:

```bash
docker logs weather_pipeline-airflow-webserver-1
```

### Database connection failed

* Ensure PostgreSQL container is running
* Verify credentials in `.env`
* If needed, reset database:

```bash
docker-compose down -v
docker-compose up -d
```

### Task stuck in retry

* Check task logs in Airflow UI
* Verify API key and city names
* Ensure MinIO bucket exists

## Future Improvements
- Replace city name queries with latitude/longitude for higher accuracy and reliability
- Integrate OpenWeather city ID dataset to avoid "city not found" issues
- Add data validation and schema enforcement before inserting into PostgreSQL
- Implement data partitioning in PostgreSQL for better performance with large datasets
- Store processed data in a data warehouse (e.g., BigQuery, Snowflake)
- Add visualization layer using tools like Power BI or Tableau
Implement alerting/monitoring (Slack, email) for failed DAG runsto CeleryExecutor or KubernetesExecutor for scalability
- Add unit tests and CI/CD pipeline for automated deployment
Optimize API calls with batching or async requests to reduce runtime

## Notes

* Use `docker-compose down` before shutting down your machine to avoid issues.
* Containers are configured with `restart: always` to recover automatically after reboot.
* For production, consider using a distributed executor such as CeleryExecutor.

## License

This project is for educational purposes.
