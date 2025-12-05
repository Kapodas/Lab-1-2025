import os
import asyncio
from telegram import Bot
from telegram.error import TelegramError
from prefect.logging import get_run_logger

def send_telegram_notification(daily_data: list, date: str):
    """Отправка уведомления в Telegram"""
    logger = get_run_logger()
    
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id:
        logger.warning("Telegram credentials not set, skipping notification")
        return
    
    try:
        # Создаем асинхронную функцию
        async def send_async():
            bot = Bot(token=token)
            
            message = f"🌤 Прогноз погоды на {date}\n\n"
            
            for record in daily_data:
                city = record.get('city', 'Unknown')
                temp_max = record.get('temp_max', 0)
                temp_min = record.get('temp_min', 0)
                precip = record.get('precipitation_total', 0)
                
                message += f"📍 {city}:\n"
                message += f"   • Макс: {temp_max}°C\n"
                message += f"   • Мин: {temp_min}°C\n"
                message += f"   • Осадки: {precip} мм\n"
                
                # Предупреждения
                warnings = []
                if precip > 10:
                    warnings.append("🌧 Сильные осадки")
                elif precip > 5:
                    warnings.append("🌦 Осадки")
                if temp_max > 30:
                    warnings.append("🔥 Жаркая погода")
                if temp_min < -10:
                    warnings.append("❄️ Сильный холод")
                elif temp_min < 0:
                    warnings.append("🥶 Мороз")
                
                if warnings:
                    message += f"   ⚠️ {' | '.join(warnings)}\n"
                
                message += "\n"
            
            await bot.send_message(chat_id=chat_id, text=message)
        
        # Запускаем асинхронную функцию
        asyncio.run(send_async())
        logger.info("Уведомление отправлено в Telegram")
        
    except TelegramError as e:
        logger.error(f"Ошибка отправки в Telegram: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при отправке в Telegram: {e}")