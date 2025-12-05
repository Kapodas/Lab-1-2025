from prefect import task
from prefect.logging import get_run_logger
from minio import Minio
import io
import json
import os

@task
def save_to_minio(data: dict, date: str):
    """Сохранение сырых данных в MinIO"""
    logger = get_run_logger()
    
    try:
        client = Minio(
            os.getenv("MINIO_ENDPOINT", "minio:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=False
        )
        
        bucket_name = os.getenv("MINIO_BUCKET", "weather-raw")
        
        # Создание бакета если не существует
        try:
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
                logger.info(f"Создан бакет {bucket_name}")
        except Exception as e:
            logger.error(f"Ошибка MinIO при создании бакета: {e}")
            raise
        
        # Сохранение JSON
        file_name = f"{data['city']}_{date}.json"
        data_str = json.dumps(data, indent=2)
        
        # Используем BytesIO для передачи данных
        data_bytes = data_str.encode('utf-8')
        data_stream = io.BytesIO(data_bytes)
        
        client.put_object(
            bucket_name,
            file_name,
            data=data_stream,
            length=len(data_bytes),
            content_type='application/json'
        )
        logger.info(f"Файл {file_name} сохранен в MinIO")
        
    except Exception as e:
        logger.error(f"Ошибка при сохранении в MinIO: {e}")
        # Не прерываем весь пайплайн из-за ошибки MinIO
        logger.warning("Продолжаем выполнение несмотря на ошибку MinIO")