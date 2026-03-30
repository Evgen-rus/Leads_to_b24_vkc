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
from typing import Dict, Any  # Для типизации данных

import requests  # Для HTTP-запросов в Битрикс24
import pandas as pd  # Для работы с Excel файлами
from tkinter import Tk, filedialog  # Для создания диалогового окна выбора файла
from dotenv import load_dotenv  # Для загрузки переменных окружения из .env

# Импортируем наш логгер
from setup import logger  # Логгер для записи событий

# Загружаем переменные окружения из .env (если файл существует)
load_dotenv()


def send_to_bitrix24(lead_data: Dict[str, Any], config: Dict[str, str] | None = None) -> bool:
    """
    Отправляет данные лида в Битрикс24 через REST API.

    Args:
        lead_data (dict): Данные о лиде
        config (dict, optional): Дополнительные настройки для Битрикс24

    Returns:
        bool: True, если отправка прошла успешно, иначе False
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
                return False

            config = {
                "webhook_url": webhook_url
            }

        # Получаем телефон из данных
        phone = lead_data.get("phone", "")

        # Формируем данные для создания лида
        lead_payload = {
            "fields": {
                "TITLE": f"LR_конк_ {phone}",  # Название лида
                "PHONE": [{"VALUE": phone, "VALUE_TYPE": "WORK"}],  # Телефон
                "SOURCE_ID": "106",
                "STATUS_ID": "UC_LF7L5W",
                "ASSIGNED_BY_ID": "20140",
            }
        }

        # Добавляем комментарий, если он есть
        if "comments" in lead_data:
            lead_payload["fields"]["COMMENTS"] = lead_data["comments"]

        logger.info(f"Отправка запроса на создание лида в Битрикс24")
        logger.info(f"Данные лида: {lead_payload}")

        # Отправляем запрос в Битрикс24
        response = requests.post(
            config["webhook_url"],
            json=lead_payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )

        # Логируем ответ от сервера для отладки
        logger.info(f"Ответ сервера: {response.status_code} - {response.text}")

        if 200 <= response.status_code < 300:
            result = response.json()
            lead_id = result.get("result")

            if not lead_id:
                raise ValueError("Не удалось получить ID лида из ответа Битрикс24")

            logger.info(f"Лид успешно создан в Битрикс24, ID: {lead_id}")

            return True
        else:
            error_message = (
                f"Ошибка при создании лида. Код ответа: {response.status_code}, "
                f"ответ: {response.text}"
            )
            logger.error(error_message)

            return False

    except Exception as e:
        error_message = f"Ошибка при отправке данных в Битрикс24: {e}"
        logger.error(error_message)

        return False

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
            
            # Пропускаем пустые строки и строки с nan (Not a Number)
            if phone and phone.lower() != 'nan':
                # Убираем .0 из номера, если телефон был распознан как число
                phone = phone.replace('.0', '')
                
                # Формируем данные лида (телефон)
                # Остальные поля (источник, этап, ответственный) настроены в bitrix24.py
                lead_data = {
                    'phone': phone
                }
                
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
            if send_to_bitrix24(lead, config):
                success += 1
                print(f"Успешно создан лид {index}/{total}: {lead['phone']}")
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
        for lead in leads[:3]:
            print(f"- Телефон: {lead['phone']}")
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