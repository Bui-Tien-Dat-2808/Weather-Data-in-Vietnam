# Weather Data Pipeline - Production-Ready End-to-End System

A modern data engineering project that implements a **Medallion Architecture** data warehouse with an end-to-end pipeline for collecting, cleaning, and analyzing weather data across Vietnam.

## Overview

This project demonstrates enterprise-grade data engineering practices:

- **Medallion Architecture**: Bronze → Silver → Gold layer data transformation
- **Clean Architecture**: Separation of concerns with clear business logic isolation
- **Data Warehouse Design**: Star Schema implementation in PostgreSQL
- **Modern Data Stack**: Airflow + MinIO + PostgreSQL + dbt + Superset
- **Production-Ready**: Proper logging, error handling, type hints, and data validation

## Architecture

### Medallion Architecture

```
┌─────────────────┐
│  OpenWeather    │
│      API        │
└────────┬────────┘
         │
         ▼
┌──────────────────────────────────────────┐
│           Bronze Layer                   │
│  Raw JSON data in MinIO (s3)            │
│  - No transformation                    │
│  - Original API response                │
└──────────────────┬───────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────┐
│           Silver Layer                   │
│  Cleaned Parquet files in MinIO         │
│  - Data validation & cleaning           │
│  - Standardized schema                  │
│  - No duplicates                        │
└──────────────────┬───────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────┐
│    Staging (PostgreSQL)                  │
│  weather_data table                     │
│  - Temporary staging layer              │
└──────────────────┬───────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────┐
│        Gold Layer (dbt Models)           │
│  ► Dimension table: dim_city            │
│  ► Fact table: fact_weather             │
│  - Star Schema for analytics            │
└──────────────────┬───────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────┐
│    BI Dashboard (Superset)               │
│  - Weather analytics & visualization    │
└──────────────────────────────────────────┘
```

### System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Airflow (Orchestration)                     │
│  ┌──────────────┬──────────────┬──────────────┬───────────────┐ │
│  │ Fetch API    │ Transform    │ Save to      │ Trigger dbt   │ │
│  │ (Bronze)     │ (Silver)     │ PostgreSQL   │ (Gold)        │ │
│  └──────────────┴──────────────┴──────────────┴───────────────┘ │
└────────────┬─────────────────────────────────────────────────────┘
             │
    ┌────────┼────────┬─────────────────┐
    ▼        ▼        ▼                 ▼
┌────────┐ ┌──────┐ ┌──────────────┐ ┌─────┐
│ MinIO  │ │ dbt  │ │ PostgreSQL   │ │ Log │
│ (S3)   │ │      │ │ Database     │ │     │
│        │ │      │ │              │ │     │
│Bronze/ │ │Marts │ │ ►dim_city    │ │Info │
│Silver  │ │Models│ │ ►fact_weather│ │Warn │
│        │ │      │ │              │ │Error│
└────────┘ └──────┘ └──────────────┘ └─────┘
```

## Project Structure

```
Weather_Pipeline/
├── src/                              # Application source code (Clean Architecture)
│   ├── domain/                       # Business logic & entities
│   │   ├── entities/
│   │   │   └── weather.py            # WeatherData, City entities
│   │   └── use_cases/                # Business use cases
│   │
│   ├── infrastructure/               # External services integration
│   │   ├── api/
│   │   │   └── openweather.py        # OpenWeather API client
│   │   └── storage/
│   │       ├── minio.py              # MinIO (S3) connector
│   │       └── postgres.py           # PostgreSQL connector
│   │
│   ├── application/                  # Business logic implementation
│   │   └── services/
│   │       ├── data_cleaning.py      # Data quality & validation
│   │       └── orchestration.py      # Pipeline orchestration
│   │
│   ├── interfaces/                   # Framework adapters (Airflow)
│   │   └── airflow/
│   │       ├── operators.py          # Airflow task operators
│   │       └── __init__.py
│   │
│   └── shared/                       # Shared utilities
│       ├── logging/
│       │   └── logger.py             # Logging configuration
│       └── config/
│           └── settings.py           # Configuration management
│
├── dags/                             # Airflow DAGs
│   └── weather_pipeline_dag.py       # Main DAG definition
│
├── dbt/                              # dbt transformations
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_weather_data.sql  # Staging model
│   │   │   └── sources.yml           # Data sources
│   │   └── marts/
│   │       ├── dim_city.sql          # Dimension table
│   │       └── fact_weather.sql      # Fact table
│   ├── tests/                        # dbt data quality tests
│   ├── macros/                       # dbt utilities
│   ├── dbt_project.yml               # dbt configuration
│   └── profiles.yml                  # dbt profile settings
│
├── plugins/                          # Airflow plugins
├── logs/                             # Airflow logs
├── tests/                            # Unit tests
│
├── docker-compose.yaml               # Services orchestration
├── .env.example                      # Environment variables template
├── requirements.txt                  # Python dependencies
└── README.md                         # This file
```

## Clean Architecture

The project follows **Clean Architecture** principles:

- **Domain Layer**: Pure business logic with no external dependencies
  - Entities: `WeatherData`, `City`, `WeatherRecord`
  - Use cases: Independent of frameworks

- **Infrastructure Layer**: External service integrations
  - API clients: OpenWeather API
  - Storage: MinIO, PostgreSQL
  - Can be swapped without affecting business logic

- **Application Layer**: Business logic orchestration
  - `DataCleaningService`: Data quality assurance
  - `WeatherPipelineOrchestrator`: Pipeline workflow

- **Interfaces Layer**: Framework adapters
  - Airflow operators
  - HTTP endpoints (future)

## Key Features

### 1. Data Pipeline (Medallion Architecture)

- **Bronze Layer**: Raw JSON data directly from API
- **Silver Layer**: Cleaned, deduplicated Parquet files
- **Gold Layer**: Star Schema fact and dimension tables

### 2. Data Quality

- **Validation**: Temperature, humidity, wind speed ranges
- **Cleaning**: Missing value handling, duplicate removal
- **Standardization**: Consistent naming, data types, timezone

### 3. Data Warehouse

**Dimension Table** (`dim_city`):
```sql
city_id | city_name | country | latitude | longitude | created_at
```

**Fact Table** (`fact_weather`):
```sql
weather_id | city_id | measurement_timestamp | temperature | humidity | wind_speed | ...
```

### 4. Logging System

Structured logging with Python's built-in logging module:
- INFO: Important operational events
- WARNING: Data quality issues
- ERROR: Critical failures

### 5. Configuration Management

All settings from environment variables:
- No hardcoded values
- Easy deployment across environments
- Safe secrets management

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Orchestration | Apache Airflow 2.10.2 | Task scheduling & DAG management |
| Object Storage | MinIO 7.1 | S3-compatible data lake (Bronze/Silver) |
| Data Warehouse | PostgreSQL 15 | Star Schema warehouse (Gold) |
| Transformations | dbt 1.5 | SQL transformation framework |
| BI Dashboard | Superset | Data visualization & analytics |
| Language | Python 3.11 | Application code |

## Setup & Installation

### Prerequisites

- Docker & Docker Compose installed
- OpenWeather API key (free tier available)

### Step 1: Clone and Prepare

```bash
cd /path/to/Weather_Pipeline
cp .env.example .env
```

### Step 2: Configure Environment

Edit `.env`:

```env
OPENWEATHER_API_KEY=your_actual_api_key
CITIES=Ha_Noi,vn;Ho_Chi_Minh,vn;Da_Nang,vn;...
```

### Step 3: Start Services

```bash
docker-compose up -d
```

Services will start:
- **Airflow WebUI**: http://localhost:8080
- **MinIO Console**: http://localhost:9001
- **Superset Dashboard**: http://localhost:8088

### Step 4: Initialize Airflow

```bash
docker-compose run --rm airflow-init
```

### Step 5: Create MinIO Bucket

```bash
# Access MinIO Console: http://localhost:9001
# Login: ROOT_USER / CHANGEME123
# Create bucket: weather-data
```

### Step 6: Configure dbt (Optional)

```bash
docker-compose exec dbt dbt run --profiles-dir /root/.dbt
```

## Running the Pipeline

### Option 1: Airflow UI

1. Open http://localhost:8080
2. Login: `airflow` / `airflow`
3. Enable DAG: `weather_data_pipeline`
4. Trigger manually or wait for schedule (every 3 hours)

### Option 2: Command Line

```bash
# Trigger DAG manually
docker-compose exec airflow-scheduler \
  airflow dags test weather_data_pipeline 2025-01-15

# View logs
docker-compose logs airflow-scheduler
docker-compose logs airflow-webserver
```

## Data Quality Checks

The silver layer performs:

| Check | Action |
|-------|--------|
| Null values | Drop rows with critical nulls |
| Temperature range | Keep -60°C to +60°C |
| Humidity range | Keep 0-100% |
| Wind speed | Keep 0-150 m/s |
| Duplicates | Remove based on city + timestamp |
| Normalize city names | Title case conversion |

## dbt Models

### Staging Models

**`stg_weather_data`**: View over raw weather_data table
- Filters out null values
- Consistent column naming
- Ordered by city and timestamp

### Mart Models

**`dim_city`** (Dimension Table)
- Unique cities from weather data
- Surrogate key: `city_id`
- Columns: `city_id`, `city_name`, `country`, `created_at`

**`fact_weather`** (Fact Table)
- Weather measurements linked to cities
- Surrogate key: `weather_id`
- Contains foreign key to `dim_city`
- Columns: `weather_id`, `city_id`, `measurement_timestamp`, `temperature`, `humidity`, ...

### Data Quality Tests

Tests ensure data integrity:
- `test_dim_city_unique_id.sql`: City IDs are unique
- `test_dim_city_not_null.sql`: City names are not null

## Best Practices Implemented

✓ Clean Architecture with clear separation of concerns  
✓ Type hints throughout the codebase  
✓ Structured logging (no print statements)  
✓ Configuration management via environment variables  
✓ Comprehensive error handling  
✓ Data validation at every layer  
✓ Modular and reusable components  
✓ SQL transformations via dbt (not pandas)  
✓ Documentation and docstrings  
✓ Production-ready Docker setup  

## Monitoring & Troubleshooting

### Check Logs

```bash
# Airflow scheduler logs
docker-compose logs airflow-scheduler | tail -100

# Airflow webserver logs
docker-compose logs airflow-webserver | tail -100

# MinIO logs
docker-compose logs minio

# PostgreSQL logs
docker-compose logs postgres
```

### Database Connection Issues

```bash
# Test PostgreSQL connection
docker-compose exec postgres psql -U airflow -d weather_db -c "SELECT 1;"

# Check weather_data table
docker-compose exec postgres psql -U airflow -d weather_db -c \
  "SELECT COUNT(*) FROM weather_data;"
```

### MinIO Issues

```bash
# List buckets
docker-compose exec minio mc ls minio

# List bronze data
docker-compose exec minio mc ls minio/weather-data/bronze

# List silver data
docker-compose exec minio mc ls minio/weather-data/silver
```

## Next Steps & Enhancements

- [ ] Implement incremental dbt models
- [ ] Add data lineage with OpenLineage
- [ ] Setup dbt Cloud for CI/CD
- [ ] Add data freshness alerts (dbt tests → Slack)
- [ ] Implement slowly changing dimensions (SCD)
- [ ] Deploy to cloud (AWS, GCP, Azure)
- [ ] Add more BI dashboards
- [ ] Implement feature store for ML
- [ ] Add API endpoint for model serving
- [ ] Setup monitoring with Prometheus + Grafana

## Code Quality

### Running Tests

```bash
# Run unit tests
docker-compose run --rm airflow pytest tests/

# Run with coverage
docker-compose run --rm airflow pytest tests/ --cov=src --cov-report=html
```

### Code Style

Code follows PEP 8 standards. Format with:

```bash
# Not included by default, but recommended
docker-compose run --rm airflow pip install black
docker-compose run --rm airflow black src/
```

## Troubleshooting Common Issues

### Issue: "OPENWEATHER_API_KEY not set"

**Solution**: Ensure `.env` file exists and contains your API key:
```bash
cp .env.example .env
# Edit .env and add your API key
```

### Issue: "Table weather_data does not exist"

**Solution**: The table is created automatically on first task run. If issues persist:
```bash
docker-compose exec postgres psql -U airflow -d weather_db -f setup.sql
```

### Issue: DAG not showing in Airflow UI

**Solution**: Ensure DAG is in correct location:
```bash
# DAG must be in:
dags/weather_pipeline_dag.py
```

### Issue: MinIO bucket "weather-data" not found

**Solution**: Create bucket manually via MinIO Console or CLI:
```bash
# Via console: http://localhost:9001
# Or via CLI:
docker-compose exec minio mc mb minio/weather-data
```

## Performance Optimization Tips

1. **Increase parallelism**: Adjust `max_active_tasks_per_dag` in `airflow.cfg`
2. **Batch API calls**: Modify pipeline to fetch multiple cities in parallel
3. **Database indexes**: Already created on `city`, `timestamp`
4. **Partition strategy**: Future enhancement for large datasets
5. **Incremental loading**: Use dbt's `is_incremental()` macro

## Security Best Practices

- ✓ Environment variables for all secrets
- ✓ No API keys in code
- ✓ Secure database credentials
- ✓ MinIO access control
- ✓ Airflow RBAC enabled (can be configured)

## Contributing

Guidelines for contributing:
- Follow Clean Architecture principles
- Add type hints to all functions
- Write docstrings for public methods
- Update README for new features
- Test changes locally before committing

## License

This project is for educational purposes.

## Support & Questions

For issues or questions:
1. Check logs: `docker-compose logs [service-name]`
2. Review troubleshooting section above
3. Check Airflow task logs in WebUI
4. Inspect MinIO console for data visibility

## Deployment Checklist

Before production deployment:

- [ ] Set strong `MINIO_SECRET_KEY`
- [ ] Set strong `POSTGRES_PASSWORD`
- [ ] Configure email alerts in Airflow
- [ ] Setup backup strategy
- [ ] Configure monitoring
- [ ] Setup log aggregation
- [ ] Configure dbt Cloud CI/CD
- [ ] Load test the pipeline
- [ ] Document runbooks
- [ ] Setup on-call alerts

---

**Built with ❤️ for Data Engineering Excellence**
