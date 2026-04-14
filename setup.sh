#!/bin/bash

# Weather Data Pipeline - Setup Script
# This script initializes the entire weather data pipeline

set -e  # Exit on error

echo "==========================================="
echo "Weather Data Pipeline - Setup Script"
echo "==========================================="
echo ""

# Check if .env file exists
if [ ! -f .env ]; then
    echo "ERROR: .env file not found!"
    echo "Please create .env from .env.example:"
    echo "  cp .env.example .env"
    echo "  # Then edit .env with your API key"
    exit 1
fi

echo "Step 1: Building Docker images..."
docker-compose build

echo ""
echo "Step 2: Starting services..."
docker-compose up -d

echo ""
echo "Step 3: Waiting for services to be ready..."
sleep 10

echo ""
echo "Step 4: Initializing Airflow..."
docker-compose run --rm airflow-init

echo ""
echo "Step 5: Creating MinIO bucket..."
docker-compose exec -T minio mc mb minio/weather-data || true

echo ""
echo "Step 6: Creating database tables..."
docker-compose exec -T postgres psql -U airflow -d weather_db -c \
  "CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    temperature FLOAT NOT NULL,
    humidity INTEGER NOT NULL,
    wind_speed FLOAT NOT NULL,
    description VARCHAR(255),
    pressure INTEGER,
    clouds INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );"

echo ""
echo "==========================================="
echo "Setup Complete!"
echo "==========================================="
echo ""
echo "Services running:"
echo "  - Airflow WebUI: http://localhost:8080"
echo "    Username: airflow"
echo "    Password: airflow"
echo ""
echo "  - MinIO Console: http://localhost:9001"
echo "    Username: ROOT_USER"
echo "    Password: CHANGEME123"
echo ""
echo "  - PostgreSQL: localhost:5433"
echo "    User: airflow"
echo "    Password: airflow"
echo "    Database: weather_db"
echo ""
echo "Next steps:"
echo "  1. Open http://localhost:8080"
echo "  2. Login with airflow/airflow"
echo "  3. Enable the 'weather_data_pipeline' DAG"
echo "  4. Trigger manually to test"
echo ""
echo "To stop services:"
echo "  docker-compose down"
echo ""
