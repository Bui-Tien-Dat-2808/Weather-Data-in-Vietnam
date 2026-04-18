# Weather Data Pipeline

Pipeline thời tiết end-to-end cho dữ liệu các tỉnh/thành Việt Nam, sử dụng Airflow để orchestration, MinIO làm data lake, PostgreSQL làm staging warehouse, dbt cho transformation và Superset để trực quan hóa.

## Kiến trúc

Luồng xử lý hiện tại:

1. `fetch_weather`: gọi OpenWeather theo tọa độ tỉnh và lưu raw payload vào `s3://weather-data/raw_data/`
2. `clean_weather_data`: làm sạch, chuẩn hóa dữ liệu và lưu parquet vào `s3://weather-data/clean_data/`
3. `save_to_postgres`: nạp dữ liệu sạch vào bảng staging `weather_data`
4. `trigger_dbt`: chạy lớp transform analytics sang `dim_city` và `fact_weather`

## Stack

| Thành phần | Công nghệ | Vai trò |
|-----------|-----------|---------|
| Orchestration | Apache Airflow 2.10.2 | Điều phối DAG |
| Data lake | MinIO | Lưu `raw_data` và `clean_data` |
| Warehouse | PostgreSQL 15 | Lưu staging và data mart |
| Transformation | dbt 1.5 | Xây dựng models |
| BI | Superset | Dashboard và query |
| Ngôn ngữ | Python 3.11 | Xử lý pipeline |

## Cấu trúc thư mục

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

## Cấu hình chính

Các biến quan trọng trong `.env`:

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

## Chạy project

```bash
docker compose up -d
docker compose run --rm airflow-init
```

Các service:

- Airflow: `http://localhost:8080`
- MinIO Console: `http://localhost:9001`
- Superset: `http://localhost:8088`
- PostgreSQL từ host: `localhost:5432`

## Kết nối PostgreSQL

Từ máy host:

```text
Host: localhost
Port: 5432
Database: weather_db
Username: airflow
Password: airflow
```

Từ container nội bộ:

```text
postgresql+psycopg2://airflow:airflow@postgres:5432/weather_db
```

## MinIO layout

```text
weather-data/
├── raw_data/
└── clean_data/
```

## dbt models

- `stg_weather_data`: staging view từ `weather_data`
- `dim_city`: dimension thành phố/tỉnh
- `fact_weather`: fact thời tiết

## Lệnh hữu ích

```bash
docker compose logs airflow-scheduler
docker compose logs postgres
docker compose exec postgres psql -U airflow -d weather_db -c "SELECT COUNT(*) FROM weather_data;"
docker compose exec minio mc ls minio/weather-data/raw_data
docker compose exec minio mc ls minio/weather-data/clean_data
docker compose exec dbt dbt run --profiles-dir /root/.dbt
```

## Test

```bash
docker compose run --rm airflow pytest tests/
```

## Ghi chú

- Task Airflow làm sạch hiện tại là `clean_weather_data`
- Superset chạy ở cổng `8088`, không dùng cổng PostgreSQL
- PostgreSQL publish ra host ở `5432`
