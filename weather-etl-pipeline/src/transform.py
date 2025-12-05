from prefect import flow, task
from prefect.logging import get_run_logger
import pandas as pd

@task
def transform_hourly_data(data: dict) -> pd.DataFrame:
    """Преобразование почасовых данных"""
    hourly = data["hourly"]
    
    # Проверяем, что есть данные
    if not hourly.get("time"):
        logger = get_run_logger()
        logger.warning(f"Нет почасовых данных для города {data['city']}, возвращаем пустой DataFrame")
        return pd.DataFrame(columns=["city", "date", "hour", "temperature", "precipitation", "wind_speed", "wind_direction"])
    
    df = pd.DataFrame({
        "time": hourly["time"],
        "temperature": hourly.get("temperature_2m", [0]*len(hourly["time"])),
        "precipitation": hourly.get("precipitation", [0]*len(hourly["time"])),
        "wind_speed": hourly.get("wind_speed_10m", [0]*len(hourly["time"])),
        "wind_direction": hourly.get("wind_direction_10m", [0]*len(hourly["time"]))
    })
    
    df["time"] = pd.to_datetime(df["time"])
    df["date"] = df["time"].dt.date
    df["hour"] = df["time"].dt.hour
    df["city"] = data["city"]
    
    # Выбор нужных колонок
    df = df[["city", "date", "hour", "temperature", "precipitation", "wind_speed", "wind_direction"]]
    
    return df

@task
def transform_daily_data(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """Агрегация дневных данных"""
    if hourly_df.empty:
        logger = get_run_logger()
        logger.warning("Пустой DataFrame для агрегации, возвращаем пустой DataFrame")
        return pd.DataFrame(columns=["city", "date", "temp_min", "temp_max", "temp_avg", "precipitation_total"])
    
    daily_df = hourly_df.groupby(["city", "date"]).agg({
        "temperature": ["min", "max", "mean"],
        "precipitation": "sum"
    }).reset_index()
    
    # Упрощаем мультииндекс колонок
    daily_df.columns = ["city", "date", "temp_min", "temp_max", "temp_avg", "precipitation_total"]
    daily_df["temp_avg"] = daily_df["temp_avg"].round(2)
    daily_df["precipitation_total"] = daily_df["precipitation_total"].round(2)
    
    return daily_df