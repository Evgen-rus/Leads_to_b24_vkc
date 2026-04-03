import os
import random
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from setup import get_logger

# Период выгрузки (дней) — можно менять
DAYS_LOOKBACK = 3

# Имя файла базы данных в корне проекта
DB_PATH = "lr182.db"

# Диапазон скопируем весь лист
GOOGLE_SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# Время в таблице указано в МСК (UTC+3)
MSK_TZ = timezone(timedelta(hours=3))

# Повторы при временных ошибках API
MAX_API_RETRIES = 4
API_RETRY_BACKOFF_SECONDS = 1.0

logger = get_logger(__file__)


def get_env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"В переменной окружения {name} пусто или нет значения")
    return value


def create_sheets_service(credentials_file: str):
    if not os.path.exists(credentials_file):
        raise FileNotFoundError(f"Файл credentials не найден: {credentials_file}")
    credentials = service_account.Credentials.from_service_account_file(
        credentials_file, scopes=GOOGLE_SHEETS_SCOPES
    )
    return build("sheets", "v4", credentials=credentials)


def execute_with_retries(request, action_name: str):
    last_error = None
    for attempt in range(1, MAX_API_RETRIES + 1):
        try:
            return request.execute()
        except HttpError as exc:
            status = exc.resp.status if exc.resp else None
            if status in (429, 500, 502, 503, 504):
                last_error = exc
            else:
                raise
        except OSError as exc:
            last_error = exc

        if attempt < MAX_API_RETRIES:
            base_delay = API_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            sleep_time = random.uniform(0, base_delay)
            logger.warning(
                "Временная ошибка API (%s). Повтор через %.2f сек...",
                action_name,
                sleep_time,
            )
            time.sleep(sleep_time)

    raise RuntimeError(
        f"Не удалось выполнить запрос к API ({action_name}) "
        f"после {MAX_API_RETRIES} попыток: {last_error}"
    )


def get_first_sheet_name(service, spreadsheet_id: str) -> str:
    request = service.spreadsheets().get(spreadsheetId=spreadsheet_id)
    spreadsheet = execute_with_retries(request, "get_spreadsheet_info")
    sheets = spreadsheet.get("sheets", [])
    if not sheets:
        raise ValueError("В таблице нет вкладок")
    return sheets[0]["properties"]["title"]


def read_sheet_values(
    service, spreadsheet_id: str, sheet_name: str
) -> List[List[str]]:
    request = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=sheet_name)
    )
    result = execute_with_retries(request, "get_sheet_values")
    return result.get("values", [])


def normalize_header(header: str) -> str:
    return re.sub(r"\s+", " ", header.strip().lower())


def map_headers(headers: List[str]) -> Dict[str, int]:
    """
    Приводим заголовки к понятным ключам.
    Ожидаемые столбцы (на русском):
    - ID
    - Дата
    - Номера
    - Канал
    - Источник
    - Статус_Б24
    """
    mapped = {}
    for idx, header in enumerate(headers):
        name = normalize_header(header)
        if name == "id":
            mapped["source_id"] = idx
        elif name == "дата":
            mapped["event_dt"] = idx
        elif name == "номера":
            mapped["phone"] = idx
        elif name == "канал":
            mapped["channel"] = idx
        elif name == "источник":
            mapped["source_lead"] = idx
        elif name in {"статус_б24", "статус б24"}:
            mapped["bitrix24_info"] = idx
    return mapped


def parse_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d.%m.%Y %H:%M:%S",
        "%d.%m.%Y",
    ]
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.replace(tzinfo=MSK_TZ)
        except ValueError:
            continue
    return None


def ensure_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS leads (
            source_id TEXT PRIMARY KEY,
            event_dt TEXT NOT NULL,
            phone TEXT NOT NULL,
            channel TEXT,
            source_lead TEXT,
            bitrix24_info TEXT,
            sheet_name TEXT,
            sheet_row INTEGER,
            inserted_at TEXT
        );
        """
    )
    conn.commit()


def row_value(row: List[str], idx: Optional[int]) -> str:
    if idx is None or idx >= len(row):
        return ""
    return str(row[idx]).strip()


def insert_rows(
    conn: sqlite3.Connection,
    rows: List[Tuple[str, str, str, str, str, str, str, int, str]],
) -> int:
    if not rows:
        return 0
    before = conn.total_changes
    conn.executemany(
        """
        INSERT INTO leads (
            source_id,
            event_dt,
            phone,
            channel,
            source_lead,
            bitrix24_info,
            sheet_name,
            sheet_row,
            inserted_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            event_dt = excluded.event_dt,
            phone = excluded.phone,
            channel = excluded.channel,
            source_lead = excluded.source_lead,
            bitrix24_info = CASE
                WHEN TRIM(excluded.bitrix24_info) <> '' THEN excluded.bitrix24_info
                ELSE leads.bitrix24_info
            END,
            sheet_name = excluded.sheet_name,
            sheet_row = excluded.sheet_row
        """,
        rows,
    )
    conn.commit()
    return conn.total_changes - before


def main() -> None:
    load_dotenv()

    sheet_id = get_env_required("GOOGLE_SHEET_ID")
    credentials_file = get_env_required("GOOGLE_CREDENTIALS_FILE")
    sheet_name_env = os.getenv("GOOGLE_SHEET_NAME", "").strip()

    spreadsheet_id = sheet_id.strip()
    if not re.fullmatch(r"[A-Za-z0-9-_]{20,}", spreadsheet_id):
        raise ValueError(
            "GOOGLE_SHEET_ID должен быть только ID таблицы без URL"
        )

    service = create_sheets_service(credentials_file)
    sheet_name = sheet_name_env or get_first_sheet_name(service, spreadsheet_id)
    logger.info("Используем вкладку: %s", sheet_name)

    data = read_sheet_values(service, spreadsheet_id, sheet_name)
    if not data:
        logger.warning("Вкладка пустая. Нечего записывать.")
        return

    headers = data[0]
    rows = data[1:]
    header_map = map_headers(headers)

    required_keys = {"source_id", "event_dt", "phone"}
    if not required_keys.issubset(set(header_map.keys())):
        missing = required_keys - set(header_map.keys())
        raise ValueError(
            f"Не найдены обязательные столбцы: {', '.join(sorted(missing))}"
        )

    cutoff = datetime.now(MSK_TZ) - timedelta(days=DAYS_LOOKBACK)
    to_insert: List[Tuple[str, str, str, str, str, str, str, int, str]] = []
    skipped_old = 0
    skipped_bad_date = 0

    for idx, row in enumerate(rows, start=2):
        event_dt_raw = row_value(row, header_map.get("event_dt"))
        event_dt = parse_datetime(event_dt_raw)
        if not event_dt:
            skipped_bad_date += 1
            continue
        if event_dt < cutoff:
            skipped_old += 1
            continue

        source_id = row_value(row, header_map.get("source_id"))
        phone = row_value(row, header_map.get("phone"))
        channel = row_value(row, header_map.get("channel"))
        source_lead = row_value(row, header_map.get("source_lead"))
        bitrix24_info = row_value(row, header_map.get("bitrix24_info"))

        if not source_id or not phone:
            continue

        to_insert.append(
            (
                source_id,
                event_dt.strftime("%Y-%m-%d %H:%M:%S"),
                phone,
                channel,
                source_lead,
                bitrix24_info,
                sheet_name,
                idx,
                datetime.now(MSK_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            )
        )

    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_db(conn)
        inserted = insert_rows(conn, to_insert)
    finally:
        conn.close()

    logger.info("Строк в таблице (без заголовка): %s", len(rows))
    logger.info(
        "Будет записано (за последние %s дней): %s", DAYS_LOOKBACK, inserted
    )
    logger.info("Пропущено (старые даты): %s", skipped_old)
    logger.info("Пропущено (не распознали дату): %s", skipped_bad_date)
    logger.info("База сохранена: %s", DB_PATH)


if __name__ == "__main__":
    main()
