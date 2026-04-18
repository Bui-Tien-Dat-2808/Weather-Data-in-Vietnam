import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from requests.exceptions import ConnectionError, RequestException, Timeout

from src.domain.entities.weather import WeatherData
from src.shared.config.mappings import PROVINCE_COORDINATES
from src.shared.config.settings import settings


logger = logging.getLogger(__name__)


class OpenWeatherAPIClient:
    """Client for fetching current weather data from OpenWeather."""

    def __init__(
        self,
        api_key: str = settings.OPENWEATHER_API_KEY,
        base_url: str = settings.OPENWEATHER_BASE_URL,
        timeout: int = settings.REQUEST_TIMEOUT,
    ):
        if not api_key:
            raise ValueError("OpenWeather API key is not provided")

        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout

        logger.info("OpenWeather API client initialized")

    def fetch_weather(self, city: str) -> Optional[Dict[str, Any]]:
        province_name = city.replace("_", " ")
        coords = PROVINCE_COORDINATES.get(province_name)

        if not coords:
            logger.warning(f"Coordinates not found for province: {province_name}")
            return None

        try:
            params = {
                'lat': coords['lat'],
                'lon': coords['lon'],
                'appid': self.api_key,
                'units': 'metric',
            }

            logger.info(
                f"Fetching weather data for {province_name} "
                f"at lat={coords['lat']}, lon={coords['lon']}"
            )
            response = requests.get(self.base_url, params=params, timeout=self.timeout)
            response.raise_for_status()

            data = response.json()
            if data.get('cod') != 200:
                error_msg = data.get('message', 'Unknown error')
                logger.warning(f"API error for {province_name}: {error_msg}")
                return None

            data['requested_city_name'] = city
            logger.info(f"Successfully fetched weather data for {province_name}")
            return data
        except Timeout:
            logger.error(f"Request timeout for {province_name}")
            return None
        except ConnectionError as exc:
            logger.error(f"Connection error for {province_name}: {exc}")
            return None
        except RequestException as exc:
            logger.error(f"Request error for {province_name}: {exc}")
            return None
        except ValueError as exc:
            logger.error(f"JSON parsing error for {province_name}: {exc}")
            return None

    def fetch_multiple_cities(self, cities: List[str]) -> List[Dict[str, Any]]:
        results = []

        for city in cities:
            data = self.fetch_weather(city)
            if data:
                results.append(data)

        logger.info(f"Fetched weather data for {len(results)}/{len(cities)} cities")
        return results

    def transform_to_entity(self, api_response: Dict[str, Any]) -> Optional[WeatherData]:
        try:
            city_name = api_response.get('requested_city_name', api_response['name']).replace("_", " ")

            return WeatherData(
                city=city_name,
                timestamp=datetime.utcfromtimestamp(api_response['dt']),
                temperature=api_response['main']['temp'],
                humidity=api_response['main']['humidity'],
                wind_speed=api_response['wind']['speed'],
                description=api_response['weather'][0]['description'] if api_response.get('weather') else None,
                pressure=api_response['main'].get('pressure'),
                clouds=api_response.get('clouds', {}).get('all'),
            )
        except (KeyError, ValueError, IndexError) as exc:
            logger.error(f"Failed to transform API response to entity: {exc}")
            return None
