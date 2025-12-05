from prefect import flow
from prefect.logging import get_run_logger
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv
from notify import send_telegram_notification
from load import load_to_clickhouse
from extract import extract_weather_data
from transform import transform_hourly_data, transform_daily_data
from save import save_to_minio
load_dotenv()

# Конфигурация
CITIES = [
    {"name": "Moscow", "lat": 55.7558, "lon": 37.6173},
    {"name": "Samara", "lat": 53.1959, "lon": 50.1002}
]

@flow(name="weather_etl", retries=2, retry_delay_seconds=30)
def weather_etl_flow():
    """Основной ETL flow"""
    logger = get_run_logger()
    
    # Дата на завтра
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"Запуск ETL пайплайна для даты: {tomorrow}")
    
    all_daily_data = []
    
    for city in CITIES:
        logger.info(f"Обработка города: {city['name']}")
        
        try:
            # Extract
            raw_data = extract_weather_data(city)
            
            # Save raw data
            save_to_minio(raw_data, tomorrow)
            
            # Transform
            hourly_df = transform_hourly_data(raw_data)
            daily_df = transform_daily_data(hourly_df)
            
            if not daily_df.empty:
                all_daily_data.append(daily_df)
            
            # Load
            load_to_clickhouse(hourly_df, "weather_hourly")
            load_to_clickhouse(daily_df, "weather_daily")
            
            logger.info(f"Город {city['name']} успешно обработан")
            
        except Exception as e:
            logger.error(f"Ошибка при обработке города {city['name']}: {e}")
            logger.warning(f"Продолжаем со следующим городом")
            continue
    
    # Prepare notification
    if all_daily_data:
        combined_daily = pd.concat(all_daily_data)
        notification_data = combined_daily.to_dict('records')
        send_telegram_notification(notification_data, tomorrow)
    else:
        logger.warning("Нет данных для отправки уведомления")
    
    logger.info("ETL пайплайн завершен")

if __name__ == "__main__":
    weather_etl_flow()