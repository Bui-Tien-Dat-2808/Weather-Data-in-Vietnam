"""
Unit tests for data cleaning service
"""
import pytest
from datetime import datetime
from src.domain.entities.weather import WeatherData
from src.application.services.data_cleaning import DataCleaningService


@pytest.fixture
def cleaning_service():
    """Create a cleaning service instance for testing."""
    return DataCleaningService()


@pytest.fixture
def valid_weather_data():
    """Create valid weather data for testing."""
    return WeatherData(
        city='Ho Chi Minh City',
        timestamp=datetime(2025, 1, 15, 12, 0, 0),
        temperature=28.5,
        humidity=75,
        wind_speed=3.2,
        description='Scattered Clouds',
        pressure=1013,
        clouds=40
    )


@pytest.fixture
def invalid_weather_data():
    """Create invalid weather data for testing."""
    return WeatherData(
        city='Test City',
        timestamp=datetime(2025, 1, 15, 12, 0, 0),
        temperature=100.0,  # Invalid: too hot
        humidity=150,        # Invalid: humidity > 100
        wind_speed=200.0,   # Invalid: too fast
        description='Test',
        pressure=1013,
        clouds=40
    )


def test_validate_valid_record(cleaning_service, valid_weather_data):
    """Test validation of valid weather record."""
    assert cleaning_service._validate_record(valid_weather_data) is True


def test_validate_invalid_temperature(cleaning_service, invalid_weather_data):
    """Test validation fails for invalid temperature."""
    assert cleaning_service._validate_record(invalid_weather_data) is False


def test_clean_normalizes_city_name(cleaning_service, valid_weather_data):
    """Test that city name is normalized."""
    # Lowercase input
    valid_weather_data.city = 'ho chi minh city'
    cleaned = cleaning_service._clean_record(valid_weather_data)
    assert cleaned.city == 'Ho Chi Minh City'


def test_clean_rounds_numbers(cleaning_service, valid_weather_data):
    """Test that numeric values are rounded."""
    valid_weather_data.temperature = 28.12345
    valid_weather_data.wind_speed = 3.56789
    cleaned = cleaning_service._clean_record(valid_weather_data)
    assert cleaned.temperature == 28.12
    assert cleaned.wind_speed == 3.57


def test_clean_weather_data_removes_invalid(cleaning_service, valid_weather_data, invalid_weather_data):
    """Test that invalid records are removed."""
    weather_list = [valid_weather_data, invalid_weather_data]
    cleaned, removed = cleaning_service.clean_weather_data(weather_list)
    
    assert len(cleaned) == 1
    assert removed == 1
    assert cleaned[0].city == 'Ho Chi Minh City'


def test_convert_to_dataframe(cleaning_service, valid_weather_data):
    """Test conversion to DataFrame."""
    weather_list = [valid_weather_data]
    df = cleaning_service.convert_to_dataframe(weather_list)
    
    assert len(df) == 1
    assert df['city'].iloc[0] == 'Ho Chi Minh City'
    assert df['temperature'].iloc[0] == 28.5
