from prefect import task
from prefect.logging import get_run_logger
import pandas as pd
import clickhouse_connect
import os
@task
def load_to_clickhouse(df: pd.DataFrame, table_name: str):
    """Загрузка данных в ClickHouse"""
    logger = get_run_logger()
    
    if df.empty:
        logger.warning(f"Пустой DataFrame, пропускаем загрузку в {table_name}")
        return
    
    try:
        # Используем порт 8123 для HTTP соединения
        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USER", "admin"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "password"),
            database=os.getenv("CLICKHOUSE_DB", "weather_db")
        )
        
        # Проверяем соединение
        client.ping()
        
        # Загружаем данные
        client.insert_df(f"weather_db.{table_name}", df)
        logger.info(f"Успешно загружено {len(df)} строк в таблицу {table_name}")
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке в ClickHouse: {e}")
        # Не прерываем весь пайплайн из-за ошибки базы данных
        logger.warning("Продолжаем выполнение несмотря на ошибку ClickHouse")
