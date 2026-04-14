"""
Architecture Documentation - Design Decisions and Rationale

This document explains the architectural choices made in the Weather Data Pipeline
and their justification from production standards perspective.
"""

# ============================================================================
# CLEAN ARCHITECTURE IMPLEMENTATION
# ============================================================================

## Motivation
Clean Architecture separates concerns into independent layers, making the codebase:
- Testable: Business logic doesn't depend on frameworks
- Maintainable: Changes to one layer don't affect others
- Flexible: Easy to swap implementations (e.g., MinIO → S3)

## Layer Structure

### 1. Domain Layer (Business Logic)
Files: src/domain/entities/*.py

Responsibility: Pure business objects independent of any infrastructure

Example: WeatherData entity
- Contains data validation logic: is_valid()
- No external dependencies
- No framework coupling

Why: If tomorrow you need to use DynamoDB instead of PostgreSQL, 
     domain logic remains unchanged.

### 2. Infrastructure Layer (External Services)
Files: src/infrastructure/**/*.py

Responsibility: Integrate with external systems

Components:
- API Client: Talks to OpenWeather API
- Storage Connectors: MinIO, PostgreSQL clients
- Handles credentials, connection pooling, error recovery

Why: Isolates external dependencies, making system flexible for migrations.

### 3. Application Layer (Business Logic Orchestration)
Files: src/application/services/*.py

Responsibility: Implement business use cases using domain and infrastructure

Example: WeatherPipelineOrchestrator
- Coordinates data flow: Fetch → Clean → Store
- Implements data quality rules
- Uses infrastructure components

Why: Single place where business logic lives, independent of execution context.

### 4. Interfaces Layer (Framework Adapters)
Files: src/interfaces/airflow/*.py

Responsibility: Connect application logic to Airflow

Example: PipelineOperations.fetch_weather_data()
- Wraps orchestration service
- Handles Airflow-specific details (XCom, context)
- Translates Airflow concepts to domain concepts

Why: Airflow is an implementation detail. If you need Prefect/Dask,
     only interfaces/airflow/* changes, not the entire business logic.

### 5. Shared Layer (Cross-cutting Concerns)
Files: src/shared/**/*.py

Responsibility: Configuration, logging, utilities available everywhere

Components:
- Settings: Centralized env var management
- Logger: Structured logging

Why: Consistent behavior across all layers.

# ============================================================================
# MEDALLION ARCHITECTURE
# ============================================================================

## Why Medallion?

Traditional ETL (one stage) vs Medallion (three stages):

TRADITIONAL:          MEDALLION:
┌────────┐           ┌────────┐
│ Raw    │──────────►│ Refined│     ┌────────────┐
│ Data   │     ETL   │ Data   │────►│ Analytics  │
└────────┘           └────────┘     └────────────┘
                             △
                             │
                      ┌──────┴───┐
                      │           │
                   Bronze        Silver
                   (Raw)         (Clean)
                                    │
                                    ▼
                                ┌──────────┐
                                │   Gold   │
                                │(Warehouse│
                                └──────────┘

Advantages:
1. **Traceability**: Can debug issues by checking each layer
2. **Separation of Concerns**: Each layer has one responsibility
3. **Reusability**: Silver data accessible for multiple gold transforms
4. **Data Governance**: Clear audit trail and data lineage
5. **Scalability**: Scale each layer independently

## Layer Responsibilities

### Bronze (Raw)
- Original API response as-is (minimal transformation)
- Immutable archive
- Format: JSON
- Location: MinIO s3://weather-data/bronze/

### Silver (Cleaned)
- Deduplicated, validated, standardized
- Ready for analytics
- Format: Parquet (better for analytics than JSON)
- Location: MinIO s3://weather-data/silver/
- Transformation Logic: DataCleaningService

### Gold (Warehouse)
- Dimensional model (Star Schema)
- Optimized for queries
- Format: PostgreSQL tables
- Dimensions & Facts
- Transformation Logic: dbt SQL models

## Data Flow Integration

API Response
    ↓
Bronze (JSON) ← MinIO raw storage
    ↓
DataCleaning (validation, normalization)
    ↓
Silver (Parquet) ← MinIO processed storage
    ↓
PostgreSQL Staging (weather_data table)
    ↓
dbt SQL Transformation
    ↓
Gold (dim_city, fact_weather)
    ↓
Superset BI Dashboard

# ============================================================================
# DATA MODELING - STAR SCHEMA
# ============================================================================

## Why Star Schema?

Simple alternatives:
1. Snowflake: More normalized, slower queries
2. Denormalized: Faster, but data anomalies
3. Star Schema: Balance of normalization and query performance

```
              ┌──────────────┐
              │  dim_city    │
              │──────────────│
              │ city_id (PK) │
              │ city_name    │
              │ country      │◄──────────┐
              │ latitude     │           │ FK
              │ longitude    │           │
              └──────────────┘           │
                    ▲                    │
                    │ FK                 │
              ┌─────┴──────────────┐     │
              │  fact_weather      │─────┘
              │────────────────────│
              │ weather_id (PK)    │
              │ city_id (FK)       │
              │ measurement_ts     │
              │ temperature        │
              │ humidity           │
              │ wind_speed         │
              │ description        │
              │ pressure           │
              │ clouds             │
              └────────────────────┘
```

Benefits:
- Fast aggregations (GROUP BY city, month)
- Simple joins (only one level)
- Scalable (add measures without changing structure)

Example Query:
```sql
SELECT 
    dc.city_name,
    EXTRACT(MONTH FROM fw.measurement_timestamp) as month,
    AVG(fw.temperature) as avg_temp,
    AVG(fw.humidity) as avg_humidity
FROM fact_weather fw
JOIN dim_city dc ON fw.city_id = dc.city_id
GROUP BY dc.city_name, month
```

# ============================================================================
# dbt TRANSFORMATIONS
# ============================================================================

## Why dbt for Gold Layer?

### Instead of pandas.to_sql()?

PANDAS:
```python
df = load_from_postgres()
df['new_col'] = df.apply(lambda x: ...)
df.to_sql('table', engine)  # Issue: Hard to version, test, debug
```

DBT:
```sql
-- models/marts/fact_weather.sql
SELECT 
    row_number() OVER (...) as weather_id,
    ... -- Clear, versionable, testable
```

### Advantages of dbt:
1. **SQL-native**: Write in SQL, not Python
2. **Version Control**: .git tracks transformations
3. **Documentation**: Automatic docs generation
4. **Tests**: Built-in data quality tests
5. **Lineage**: Tracks data dependencies
6. **Modularity**: ref() for reusable models
7. **CI/CD**: Easy to integrate with CI/CD pipelines

## Model Structure

```
dbt/models/
├── staging/
│   ├── stg_weather_data.sql  ← Views (lightweight)
│   └── sources.yml           ← Data source definitions
│
└── marts/
    ├── dim_city.sql          ← Dimension table
    └── fact_weather.sql      ← Fact table
```

Staging: Lightweight transformations, close to source
Marts: Business-ready, optimized tables

# ============================================================================
# LOGGING STRATEGY
# ============================================================================

## Problem with print() statements:
- No timestamps
- No log levels
- Can't disable easily
- Hard to parse
- No structured format

## Solution: Python logging module

```python
from src.shared.logging.logger import get_logger

logger = get_logger(__name__)
logger.info("Processing started")
logger.warning("Data quality issue")
logger.error("Critical failure")
```

Output format:
```
2025-01-15 12:00:00 - src.infrastructure.api.openweather - INFO - Fetching weather data for Ho Chi Minh
```

Benefits:
- Timestamps
- Log levels (INFO < WARNING < ERROR)
- Module names
- Easy parsing for logging aggregation (ELK, Splunk)
- Production-ready

# ============================================================================
# CONFIGURATION MANAGEMENT
# ============================================================================

## Problem:
- Hardcoded values → Hard to deploy
- Secrets in code → Security risk
- Different configs for dev/prod → Error prone

## Solution: Environment Variables + Settings class

```python
# src/shared/config/settings.py
class Settings:
    API_KEY = os.getenv('OPENWEATHER_API_KEY')
    MIN_TEMP = float(os.getenv('MIN_VALID_TEMPERATURE', -60.0))
    ...

# Usage anywhere:
from src.shared.config.settings import settings
logger.info(f"API Key configured: {settings.API_KEY[:10]}...")  # Safe
```

Benefits:
- Single source of truth
- Validation on startup: settings.validate()
- Easy to override per environment
- Secure (keep .env out of git)
- Typed (for IDE hints)

# ============================================================================
# ERROR HANDLING & RESILIENCE
# ============================================================================

## Retry Strategy

Airflow DAG configuration:
```python
retry_delay=timedelta(minutes=5),  # Wait 5 min before retry
retries=2,                         # Try 3 times total
```

Applied to:
- fetch_weather: 2 retries (API might be temporary down)
- transform_to_silver: 1 retry (data is stable)
- save_to_postgres: 1 retry (DB might recover)
- trigger_dbt: 1 retry (model might need re-run)

## Error Logging

Every exception is logged with context:
```python
except ClientError as e:
    logger.error(f"Failed to upload file to MinIO: {e}")
    raise  # Re-raise for Airflow to handle
```

This provides:
- Exact error message
- Stack trace (via raise)
- Context (what operation failed)
- Audit trail (in logs)

# ============================================================================
# DATA VALIDATION
# ============================================================================

## Three Levels of Validation

### 1. Domain Level (WeatherData entity)
```python
def is_valid(self, min_temp=-60, max_temp=60, ...) -> bool:
    return (
        min_temp <= self.temperature <= max_temp and
        ...
    )
```
Business rules validation.

### 2. Application Level (DataCleaningService)
```python
def _validate_record(self, weather: WeatherData) -> bool:
    # Check ranges
    # Check nulls
    # Check types
```
Data quality assurance.

### 3. dbt Level
```sql
-- tests/test_dim_city_unique_id.sql
SELECT city_id FROM dim_city GROUP BY city_id HAVING COUNT(*) > 1
```
Warehouse integrity tests.

Benefits:
- Catch bugs at every stage
- Clear responsibility per layer
- Fail fast (don't propagate bad data)

# ============================================================================
# DOCKER STRATEGY
# ============================================================================

## Services

| Service | Container | Purpose |
|---------|-----------|---------|
| airflow-scheduler | Airflow | Orchestration engine |
| airflow-webserver | Airflow | UI (port 8080) |
| postgres | PostgreSQL | Database |
| minio | MinIO | Object storage (port 9001 console) |
| dbt | dbt | Transformation runner |
| superset | Superset | BI Dashboard (port 8088) |

## Volume Mounts

- dags/ → /opt/airflow/dags (DAGs auto-reload)
- src/ → /opt/airflow/src (business logic available)
- dbt/ → /root/dbt (transforms available)

Why: Changes to Python code or SQL immediately available.

## Network

Docker Compose creates network bridge. Services can reference each other:
```
postgres hostname: "postgres"     (not "localhost")
minio endpoint: "http://minio:9000"
```

# ============================================================================
# DEPLOYMENT CONSIDERATIONS
# ============================================================================

## Development vs Production

| Aspect | Dev | Production |
|--------|-----|-----------|
| Secrets | .env file | AWS Secrets Manager / Vault |
| Database | PostgreSQL container | Managed RDS |
| Storage | MinIO container | AWS S3 |
| Orchestration | LocalExecutor | CeleryExecutor / KubernetesExecutor |
| Monitoring | Manual logs | Prometheus + Grafana |
| Alerting | None | PagerDuty / Slack |
| Backups | Manual | Automated snapshots |

## Migration Path to Cloud

1. Swap MinIOConnector with S3 connector (interface unchanged)
2. Swap PostgreSQLConnector with RDS (connection string only)
3. Deploy Airflow on Kubernetes (DAG remains same)
4. Add dbt Cloud for transformations
5. Replace Superset with cloud BI (Redshift + QuickSight, etc.)

Business logic persists across all migrations because of Clean Architecture!

# ============================================================================
# CONCLUSION
# ============================================================================

This architecture prioritizes:
1. **Maintainability**: Clear separation of concerns
2. **Testability**: No tight coupling to frameworks
3. **Scalability**: Each layer can be optimized independently
4. **Production Readiness**: Logging, errors, configuration, monitoring
5. **Flexibility**: Easy to swap implementations
6. **Data Quality**: Validation at every layer
7. **Documentation**: Self-explanatory code with clear patterns

These principles align with industry best practices and prepare the codebase
for enterprise deployment and scaling.
"""
