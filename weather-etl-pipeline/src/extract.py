from prefect import task
from prefect.logging import get_run_logger
import requests
from datetime import datetime, timedelta
import pandas as pd
import os


@task(retries=3, retry_delay_seconds=10)
def extract_weather_data(city: dict) -> dict:
    """Извлечение данных из Open-Meteo API для завтрашнего дня"""
    logger = get_run_logger()
    url = os.getenv("OPENMETEO_BASE_URL", "https://api.open-meteo.com/v1/forecast")
    
    # Запрашиваем прогноз на 2 дня (сегодня и завтра)
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "hourly": "temperature_2m,precipitation,wind_speed_10m,wind_direction_10m",
        "forecast_days": 2,
        "timezone": "Europe/Moscow"
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Данные для {city['name']} успешно получены")
        
        # Берем только данные на завтра
        tomorrow = (datetime.now() + timedelta(days=1)).date()
        
        # Создаем DataFrame для удобной фильтрации
        hourly_data = data["hourly"]
        df = pd.DataFrame({
            "time": hourly_data["time"],
            "temperature_2m": hourly_data.get("temperature_2m", []),
            "precipitation": hourly_data.get("precipitation", []),
            "wind_speed_10m": hourly_data.get("wind_speed_10m", []),
            "wind_direction_10m": hourly_data.get("wind_direction_10m", [])
        })
        
        # Конвертируем время в datetime
        df["time"] = pd.to_datetime(df["time"])
        df["date"] = df["time"].dt.date
        
        # Фильтруем только завтрашние данные
        tomorrow_df = df[df["date"] == tomorrow].copy()
        
        if tomorrow_df.empty:
            logger.warning(f"Нет данных на завтра для города {city['name']}")
            # Возвращаем пустые данные
            filtered_data = {
                "city": city["name"],
                "latitude": data["latitude"],
                "longitude": data["longitude"],
                "hourly": {
                    "time": [],
                    "temperature_2m": [],
                    "precipitation": [],
                    "wind_speed_10m": [],
                    "wind_direction_10m": []
                }
            }
        else:
            # Собираем отфильтрованные данные
            filtered_data = {
                "city": city["name"],
                "latitude": data["latitude"],
                "longitude": data["longitude"],
                "hourly": {
                    "time": tomorrow_df["time"].dt.strftime("%Y-%m-%dT%H:%M").tolist(),
                    "temperature_2m": tomorrow_df["temperature_2m"].tolist(),
                    "precipitation": tomorrow_df["precipitation"].tolist(),
                    "wind_speed_10m": tomorrow_df["wind_speed_10m"].tolist(),
                    "wind_direction_10m": tomorrow_df["wind_direction_10m"].tolist()
                }
            }
        
        logger.info(f"Отфильтровано {len(filtered_data['hourly']['time'])} часов для завтрашнего дня")
        return filtered_data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при получении данных для {city['name']}: {e}")
        raise
    except KeyError as e:
        logger.error(f"Отсутствует ключ в ответе API для {city['name']}: {e}")
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка при обработке данных для {city['name']}: {e}")
        raise