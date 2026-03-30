"""
Модуль конфигурации проекта.
Отвечает за настройку логирования.
"""
import logging
from pathlib import Path

# Базовые пути
# BASE_DIR - директория, где лежит этот файл (корень проекта)
BASE_DIR = Path(__file__).resolve().parent
LOGS_DIR = BASE_DIR / 'logs'

# Создаем необходимые директории
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Настройки для логирования
LOG_FILE = str(LOGS_DIR / 'leads_to_b24.log')
LOG_LEVEL = logging.INFO

def setup_logging():
    """
    Настройка системы логирования.
    
    Returns:
        logging.Logger: Настроенный логгер
    """
    # Создаем логгер
    logger = logging.getLogger('leads_to_b24')
    logger.setLevel(logging.DEBUG)
    
    # Форматтер для логов
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Файловый handler - записывает все логи
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Консольный handler - только важные сообщения
    console_handler = logging.StreamHandler()
    console_handler.setLevel(LOG_LEVEL)
    console_handler.setFormatter(formatter)
    
    # Фильтр для консоли - пропускаем только сообщения о создании лидов
    class LeadCreationFilter(logging.Filter):
        def filter(self, record):
            return "Был создан лид в Битрикс24" in record.getMessage()
    
    console_handler.addFilter(LeadCreationFilter())
    logger.addHandler(console_handler)
    
    return logger

# Создание логгера
logger = setup_logging() 