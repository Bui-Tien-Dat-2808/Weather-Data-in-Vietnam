"""
OpenWeather API client for fetching weather data.
"""
import logging
from typing import List, Optional, Dict, Any

import requests
from requests.exceptions import RequestException, Timeout, ConnectionError

from src.shared.config.settings import settings
from src.domain.entities.weather import WeatherData


logger = logging.getLogger(__name__)


class OpenWeatherAPIClient:
    """
    Client for interacting with OpenWeather API.
    Fetches current weather data for specified cities.
    """

    def __init__(self,
                 api_key: str = settings.OPENWEATHER_API_KEY,
                 base_url: str = settings.OPENWEATHER_BASE_URL,
                 timeout: int = settings.REQUEST_TIMEOUT):
        """
        Initialize OpenWeather API client.

        Args:
            api_key: OpenWeather API key
            base_url: Base URL for API calls
            timeout: Request timeout in seconds
        """
        if not api_key:
            raise ValueError("OpenWeather API key is not provided")

        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout

        logger.info("OpenWeather API client initialized")

    def fetch_weather(self, city: str, country: str = 'vn') -> Optional[Dict[str, Any]]:
        """
        Fetch weather data for a city.

        Args:
            city: City name (with spaces replaced by underscores)
            country: Country code (default: 'vn' for Vietnam)

        Returns:
            Raw API response as dictionary, or None if request fails
        """
        # Replace underscores with spaces
        city_name = city.replace("_", " ")

        try:
            params = {
                'q': f"{city_name},{country}",
                'appid': self.api_key,
                'units': 'metric'
            }

            logger.info(f"Fetching weather data for {city_name}, {country}")
            response = requests.get(
                self.base_url,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()

            data = response.json()

            # Check API response code
            if data.get('cod') != 200:
                error_msg = data.get('message', 'Unknown error')
                logger.warning(f"API error for {city_name}: {error_msg}")
                return None

            logger.info(f"Successfully fetched weather data for {city_name}")
            return data

        except Timeout:
            logger.error(f"Request timeout for {city_name}")
            return None
        except ConnectionError as e:
            logger.error(f"Connection error for {city_name}: {e}")
            return None
        except RequestException as e:
            logger.error(f"Request error for {city_name}: {e}")
            return None
        except ValueError as e:
            logger.error(f"JSON parsing error for {city_name}: {e}")
            return None

    def fetch_multiple_cities(self, cities: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch weather data for multiple cities.

        Args:
            cities: List of city names

        Returns:
            List of successful API responses
        """
        results = []

        for city in cities:
            data = self.fetch_weather(city)
            if data:
                results.append(data)

        logger.info(f"Fetched weather data for {len(results)}/{len(cities)} cities")
        return results

    def transform_to_entity(self, api_response: Dict[str, Any]) -> Optional[WeatherData]:
        """
        Transform API response to WeatherData entity.

        Args:
            api_response: Raw API response

        Returns:
            WeatherData entity or None if transformation fails
        """
        try:
            from datetime import datetime

            return WeatherData(
                city=api_response['name'],
                timestamp=datetime.utcfromtimestamp(api_response['dt']),
                temperature=api_response['main']['temp'],
                humidity=api_response['main']['humidity'],
                wind_speed=api_response['wind']['speed'],
                description=api_response['weather'][0]['description'] if api_response.get('weather') else None,
                pressure=api_response['main'].get('pressure'),
                clouds=api_response.get('clouds', {}).get('all')
            )
        except (KeyError, ValueError, IndexError) as e:
            logger.error(f"Failed to transform API response to entity: {e}")
            return None
