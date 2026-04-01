"""
Скрипт для загрузки лидов из Excel-файла в Битрикс24.
Основные функции:
1. Выбор Excel файла через диалоговое окно
2. Чтение телефонов из файла
3. Отправка лидов в Битрикс24
"""

# Импорт необходимых библиотек
import time  # Для создания задержек между запросами
import os    # Для работы с файловой системой
from datetime import datetime  # Для получения текущей даты
from typing import Dict, Any  # Для типизации данных
from zoneinfo import ZoneInfo  # Для часового пояса Москвы

import requests  # Для HTTP-запросов в Битрикс24
import pandas as pd  # Для работы с Excel файлами
from tkinter import Tk, filedialog  # Для создания диалогового окна выбора файла
from dotenv import load_dotenv  # Для загрузки переменных окружения из .env

# Импортируем наш логгер
from setup import logger  # Логгер для записи событий

# Загружаем переменные окружения из .env (если файл существует)
load_dotenv()

BITRIX_MAX_RETRIES = int(os.getenv("BITRIX_MAX_RETRIES", "3"))
BITRIX_RETRY_BASE_DELAY = float(os.getenv("BITRIX_RETRY_BASE_DELAY", "1"))


def build_lead_url(webhook_url: str, lead_id: str | int) -> str:
    """Собирает ссылку на карточку лида по URL вебхука и ID лида."""
    portal_url = webhook_url.split("/rest/", maxsplit=1)[0].rstrip("/")
    return f"{portal_url}/crm/lead/details/{lead_id}/"


def build_api_method_url(webhook_url: str, method: str) -> str:
    """Собирает полный URL метода API Битрикс24."""
    return f"{webhook_url.rstrip('/')}/{method}"


def is_retryable_status(status_code: int) -> bool:
    """Проверяет, стоит ли повторить запрос по коду ответа."""
    return status_code == 429 or 500 <= status_code < 600


def send_to_bitrix24(
    lead_data: Dict[str, Any],
    config: Dict[str, str] | None = None,
) -> str | None:
    """
    Отправляет данные лида в Битрикс24 через REST API.

    Args:
        lead_data (dict): Данные о лиде
        config (dict, optional): Дополнительные настройки для Битрикс24

    Returns:
        str | None: Ссылка на созданный лид или None при ошибке
    """
    try:
        # Если конфиг не передан, пробуем взять URL вебхука только из переменной окружения
        if config is None:
            webhook_url = os.getenv("BITRIX_WEBHOOK_URL")
            if not webhook_url:
                logger.error(
                    "BITRIX_WEBHOOK_URL не задан. "
                    "Укажите URL вебхука в .env или передайте его через параметр config."
                )
                return None

            config = {
                "webhook_url": webhook_url
            }

        # Получаем телефон из данных
        phone = lead_data.get("phone", "")
        title_date = datetime.now(ZoneInfo("Europe/Moscow")).strftime("%d%m%Y")

        # Формируем данные для создания лида
        lead_payload = {
            "fields": {
                "TITLE": f"Перехват лидов {title_date}",  # Название лида с датой по Москве
                "PHONE": [{"VALUE": phone, "VALUE_TYPE": "MOBILE"}],  # Телефон
                # Значения настроены под текущий портал клиента.
                "SOURCE_ID": "15",
                "STATUS_ID": "NEW",
                "UF_CRM_ROISTAT": "парсинг",
                "ASSIGNED_BY_ID": "1",
            }
        }

        # Добавляем комментарий, если он есть
        if "comments" in lead_data:
            lead_payload["fields"]["COMMENTS"] = lead_data["comments"]

        logger.info(f"Отправка запроса на создание лида в Битрикс24")
        logger.info(f"Данные лида: {lead_payload}")

        method_url = build_api_method_url(config["webhook_url"], "crm.lead.add")

        for attempt in range(1, BITRIX_MAX_RETRIES + 1):
            try:
                response = requests.post(
                    method_url,
                    json=lead_payload,
                    headers={"Content-Type": "application/json"},
                    timeout=10,
                )
            except (
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
            ) as error:
                if attempt < BITRIX_MAX_RETRIES:
                    delay = BITRIX_RETRY_BASE_DELAY * (2 ** (attempt - 1))
                    retry_message = (
                        f"Временная ошибка сети при создании лида {phone}: {error}. "
                        f"Повтор через {delay:.1f} сек. "
                        f"(попытка {attempt + 1}/{BITRIX_MAX_RETRIES})"
                    )
                    logger.warning(retry_message)
                    print(retry_message)
                    time.sleep(delay)
                    continue

                error_message = f"Ошибка при отправке данных в Битрикс24: {error}"
                logger.error(error_message)
                print(error_message)
                return None

            logger.info(f"Ответ сервера: {response.status_code} - {response.text}")

            try:
                result = response.json()
            except ValueError:
                result = {}

            if 200 <= response.status_code < 300:
                if result.get("error"):
                    error_message = (
                        "Ошибка API Битрикс24 при создании лида: "
                        f"{result.get('error_description') or result.get('error')}"
                    )
                    logger.error(error_message)
                    print(error_message)
                    return None

                lead_id = result.get("result")

                if not lead_id:
                    raise ValueError("Не удалось получить ID лида из ответа Битрикс24")

                lead_url = build_lead_url(config["webhook_url"], lead_id)
                logger.info(f"Лид успешно создан в Битрикс24: {lead_url}")

                return lead_url

            api_error = result.get("error_description") or result.get("error") or response.text

            if is_retryable_status(response.status_code) and attempt < BITRIX_MAX_RETRIES:
                delay = BITRIX_RETRY_BASE_DELAY * (2 ** (attempt - 1))
                retry_message = (
                    f"Временная ошибка API при создании лида {phone}. "
                    f"Код ответа: {response.status_code}, ответ API: {api_error}. "
                    f"Повтор через {delay:.1f} сек. "
                    f"(попытка {attempt + 1}/{BITRIX_MAX_RETRIES})"
                )
                logger.warning(retry_message)
                print(retry_message)
                time.sleep(delay)
                continue

            error_message = (
                f"Ошибка при создании лида. Код ответа: {response.status_code}, "
                f"ответ API: {api_error}"
            )
            logger.error(error_message)
            print(error_message)
            return None

    except Exception as e:
        error_message = f"Ошибка при отправке данных в Битрикс24: {e}"
        logger.error(error_message)
        print(error_message)

        return None

def read_leads_from_excel(file_path: str) -> list[Dict[str, Any]]:
    """
    Читает данные лидов из Excel-файла.
    
    Args:
        file_path (str): Путь к Excel-файлу с лидами
        
    Returns:
        list[Dict[str, Any]]: Список словарей с данными лидов
    """
    leads = []
    try:
        # Читаем Excel-файл в pandas DataFrame
        df = pd.read_excel(file_path)
        
        # Проверяем наличие обязательной колонки 'Телефон'
        required_columns = ['Телефон']
        for column in required_columns:
            if column not in df.columns:
                raise ValueError(f"В файле отсутствует колонка '{column}'")
        
        # Проходим по каждой строке Excel файла
        for index, row in df.iterrows():
            # Получаем телефон и убираем лишние пробелы
            phone = str(row['Телефон']).strip()
            comment = ""

            if "Комментарий" in df.columns:
                raw_comment = row["Комментарий"]
                if pd.notna(raw_comment):
                    comment = str(raw_comment).strip()
            
            # Пропускаем пустые строки и строки с nan (Not a Number)
            if phone and phone.lower() != 'nan':
                # Убираем .0 из номера, если телефон был распознан как число
                phone = phone.replace('.0', '')
                
                # Формируем данные лида из строки Excel.
                lead_data = {
                    'phone': phone
                }

                if comment:
                    lead_data["comments"] = comment
                
                leads.append(lead_data)
                    
        logger.info(f"Прочитано {len(leads)} лидов из файла")
        return leads
        
    except Exception as e:
        print(f"Ошибка при чтении Excel-файла: {e}")
        return []

def upload_leads_to_bitrix(leads: list[Dict[str, Any]], config: Dict[str, str]) -> None:
    """
    Загружает лиды в Битрикс24.
    
    Args:
        leads (list[Dict[str, Any]]): Список лидов для загрузки
        config (Dict[str, str]): Конфигурация для подключения к Битрикс24 (webhook_url)
    """
    total = len(leads)  # Общее количество лидов
    success = 0  # Счетчик успешно созданных лидов
    
    # Проходим по каждому лиду
    for index, lead in enumerate(leads, 1):
        try:
            # Отправляем лид в Битрикс24 через функцию send_to_bitrix24
            lead_url = send_to_bitrix24(lead, config)
            if lead_url:
                success += 1
                print(f"Создан лид {lead['phone']} {lead_url}")
            else:
                print(f"Не удалось создать лид {index}/{total}: {lead['phone']}")
            
            # Пауза 0.5 секунды между запросами, чтобы не перегрузить API Битрикс24
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Ошибка при создании лида {lead['phone']}: {e}")
    
    # Выводим итоговую статистику
    print(f"\nЗагрузка завершена. Успешно: {success}/{total}")

def select_excel_file() -> str:
    """
    Открывает диалоговое окно для выбора Excel файла.
    
    Returns:
        str: Путь к выбранному файлу или пустая строка, если файл не выбран
    """
    # Создаем корневое окно Tkinter
    root = Tk()
    root.withdraw()  # Скрываем основное окно
    root.attributes('-topmost', True)  # Окно выбора файла будет поверх других окон
    
    # Открываем диалог выбора файла
    file_path = filedialog.askopenfilename(
        title="Выберите Excel файл с лидами",
        filetypes=[("Excel files", "*.xlsx *.xls")],  # Только Excel файлы
        # Начальная директория - папка, где лежит этот скрипт (корень проекта)
        initialdir=os.path.dirname(os.path.abspath(__file__))
    )
    
    root.destroy()  # Закрываем окно Tkinter
    return file_path

def main():
    """
    Основная функция для запуска загрузки лидов.
    Последовательность действий:
    1. Выбор Excel файла через диалоговое окно
    2. Проверка существования файла
    3. Чтение лидов из файла
    4. Показ примера первых трех лидов
    5. Подтверждение загрузки
    6. Загрузка лидов в Битрикс24
    """
    # Открываем диалог выбора файла
    file_path = select_excel_file()
    
    # Проверяем, был ли выбран файл
    if not file_path:
        print("Файл не выбран. Загрузка отменена.")
        return
    
    # Проверяем существование файла
    if not os.path.exists(file_path):
        print(f"Файл не найден: {file_path}")
        return
    
    # Читаем URL вебхука Битрикс24 из переменной окружения
    webhook_url = os.getenv("BITRIX_WEBHOOK_URL")
    if not webhook_url:
        print("Ошибка: переменная окружения BITRIX_WEBHOOK_URL не задана.")
        print("Добавьте её в файл .env и укажите URL вебхука Битрикс24.")
        return

    # Конфигурация для подключения к Битрикс24
    config = {
        'webhook_url': webhook_url  # Битрикс24
    }
    
    # Читаем лиды из файла
    leads = read_leads_from_excel(file_path)
    
    if leads:
        # Показываем информацию о найденных лидах
        print(f"Найдено {len(leads)} лидов.")
        print("\nПример первых 3 лидов:")
        for index, lead in enumerate(leads[:3], start=1):
            print(f"\nЛид {index}:")
            print(f"Телефон: {lead['phone']}")
            if lead.get("comments"):
                print(f"Комментарий: {lead['comments']}")
            print("-" * 50)
            
        # Запрашиваем подтверждение на загрузку
        print("\nНачать загрузку? (y/n)")
        if input().lower() == 'y':
            upload_leads_to_bitrix(leads, config)
        else:
            print("Загрузка отменена")
    else:
        print("Не найдено лидов для загрузки")

# Точка входа в программу
if __name__ == "__main__":
    main() 