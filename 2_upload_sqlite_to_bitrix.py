import os
import random
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from setup import get_logger


load_dotenv()

DB_PATH = Path(__file__).resolve().parent / "lr182.db"
GOOGLE_SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
GOOGLE_MAX_RETRIES = 4
GOOGLE_RETRY_BACKOFF_SECONDS = 1.0

BITRIX_MAX_RETRIES = int(os.getenv("BITRIX_MAX_RETRIES", "3"))
BITRIX_RETRY_BASE_DELAY = float(os.getenv("BITRIX_RETRY_BASE_DELAY", "1"))
BITRIX_SOURCE_ID = os.getenv("BITRIX_SOURCE_ID", "15")
BITRIX_STATUS_ID = os.getenv("BITRIX_STATUS_ID", "NEW")
BITRIX_ASSIGNED_BY_ID = os.getenv("BITRIX_ASSIGNED_BY_ID", "1")
BITRIX_ROISTAT = os.getenv("BITRIX_ROISTAT", "парсинг")

logger = get_logger(__file__)


@dataclass
class LeadRecord:
    source_id: str
    phone: str
    source_lead: str
    sheet_name: str
    sheet_row: int


def get_env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Environment variable {name} is empty or missing")
    return value


def build_lead_url(webhook_url: str, lead_id: str | int) -> str:
    portal_url = webhook_url.split("/rest/", maxsplit=1)[0].rstrip("/")
    return f"{portal_url}/crm/lead/details/{lead_id}/"


def build_api_method_url(webhook_url: str, method: str) -> str:
    return f"{webhook_url.rstrip('/')}/{method}"


def is_retryable_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code < 600


def create_sheets_service(credentials_file: str):
    if not os.path.exists(credentials_file):
        raise FileNotFoundError(f"Credentials file not found: {credentials_file}")

    credentials = service_account.Credentials.from_service_account_file(
        credentials_file,
        scopes=GOOGLE_SHEETS_SCOPES,
    )
    return build("sheets", "v4", credentials=credentials)


def execute_google_request_with_retries(request, action_name: str):
    last_error = None

    for attempt in range(1, GOOGLE_MAX_RETRIES + 1):
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

        if attempt < GOOGLE_MAX_RETRIES:
            base_delay = GOOGLE_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            sleep_time = random.uniform(0, base_delay)
            logger.warning(
                "Temporary Google Sheets API error during %s. Retry in %.2f sec.",
                action_name,
                sleep_time,
            )
            time.sleep(sleep_time)

    raise RuntimeError(
        f"Google Sheets API request failed ({action_name}) "
        f"after {GOOGLE_MAX_RETRIES} attempts: {last_error}"
    )


def normalize_header(header: str) -> str:
    return re.sub(r"\s+", " ", header.strip().lower())


def quote_sheet_name(sheet_name: str) -> str:
    return "'" + sheet_name.replace("'", "''") + "'"


def column_index_to_a1(column_index: int) -> str:
    result = []
    current = column_index + 1

    while current > 0:
        current, remainder = divmod(current - 1, 26)
        result.append(chr(65 + remainder))

    return "".join(reversed(result))


def get_status_column_index(
    service,
    spreadsheet_id: str,
    sheet_name: str,
    cache: Dict[str, int],
) -> int:
    if sheet_name in cache:
        return cache[sheet_name]

    range_name = f"{quote_sheet_name(sheet_name)}!1:1"
    request = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
    )
    result = execute_google_request_with_retries(request, f"read_header:{sheet_name}")
    headers = result.get("values", [[]])
    header_row = headers[0] if headers else []

    for index, header in enumerate(header_row):
        if normalize_header(str(header)) in {"статус_б24", "статус б24"}:
            cache[sheet_name] = index
            return index

    raise ValueError(f"Column 'Статус_Б24' not found in sheet '{sheet_name}'")


def update_sheet_status(
    service,
    spreadsheet_id: str,
    sheet_name: str,
    sheet_row: int,
    status_column_index: int,
    lead_url: str,
) -> None:
    cell = f"{column_index_to_a1(status_column_index)}{sheet_row}"
    range_name = f"{quote_sheet_name(sheet_name)}!{cell}"
    request = (
        service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="RAW",
            body={"values": [[lead_url]]},
        )
    )
    execute_google_request_with_retries(request, f"update_status:{sheet_name}!{cell}")


def fetch_pending_leads(conn: sqlite3.Connection) -> list[LeadRecord]:
    rows = conn.execute(
        """
        SELECT
            source_id,
            phone,
            COALESCE(source_lead, ''),
            COALESCE(sheet_name, ''),
            sheet_row
        FROM leads
        WHERE TRIM(COALESCE(bitrix24_info, '')) = ''
        ORDER BY event_dt, source_id
        """
    ).fetchall()

    return [
        LeadRecord(
            source_id=str(row[0]).strip(),
            phone=str(row[1]).strip(),
            source_lead=str(row[2]).strip(),
            sheet_name=str(row[3]).strip(),
            sheet_row=int(row[4]) if row[4] is not None else 0,
        )
        for row in rows
    ]


def update_db_status(conn: sqlite3.Connection, source_id: str, lead_url: str) -> None:
    conn.execute(
        """
        UPDATE leads
        SET bitrix24_info = ?
        WHERE source_id = ?
        """,
        (lead_url, source_id),
    )
    conn.commit()


def send_to_bitrix24(lead: LeadRecord, webhook_url: str) -> Optional[str]:
    try:
        title_date = datetime.now(ZoneInfo("Europe/Moscow")).strftime("%d%m%Y")
        lead_payload = {
            "fields": {
                "TITLE": f"Перехват лидов {title_date}",
                "PHONE": [{"VALUE": lead.phone, "VALUE_TYPE": "MOBILE"}],
                "SOURCE_ID": BITRIX_SOURCE_ID,
                "STATUS_ID": BITRIX_STATUS_ID,
                "UF_CRM_ROISTAT": BITRIX_ROISTAT,
                "ASSIGNED_BY_ID": BITRIX_ASSIGNED_BY_ID,
            }
        }

        if lead.source_lead:
            lead_payload["fields"]["COMMENTS"] = lead.source_lead

        method_url = build_api_method_url(webhook_url, "crm.lead.add")
        logger.info("Sending lead source_id=%s phone=%s to Bitrix24", lead.source_id, lead.phone)

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
                    logger.warning(
                        "Temporary network error while creating lead source_id=%s phone=%s: %s. "
                        "Retry in %.1f sec. Attempt %s/%s",
                        lead.source_id,
                        lead.phone,
                        error,
                        delay,
                        attempt + 1,
                        BITRIX_MAX_RETRIES,
                    )
                    time.sleep(delay)
                    continue

                logger.error(
                    "Bitrix24 send failed for source_id=%s phone=%s: %s",
                    lead.source_id,
                    lead.phone,
                    error,
                )
                return None

            logger.info(
                "Bitrix24 response for source_id=%s: %s - %s",
                lead.source_id,
                response.status_code,
                response.text,
            )

            try:
                result = response.json()
            except ValueError:
                result = {}

            if 200 <= response.status_code < 300:
                if result.get("error"):
                    logger.error(
                        "Bitrix24 API error for source_id=%s: %s",
                        lead.source_id,
                        result.get("error_description") or result.get("error"),
                    )
                    return None

                lead_id = result.get("result")
                if not lead_id:
                    raise ValueError("Bitrix24 response does not contain lead ID")

                lead_url = build_lead_url(webhook_url, lead_id)
                logger.info(
                    "Lead created in Bitrix24 for source_id=%s: %s",
                    lead.source_id,
                    lead_url,
                )
                return lead_url

            api_error = result.get("error_description") or result.get("error") or response.text
            if is_retryable_status(response.status_code) and attempt < BITRIX_MAX_RETRIES:
                delay = BITRIX_RETRY_BASE_DELAY * (2 ** (attempt - 1))
                logger.warning(
                    "Temporary Bitrix24 API error for source_id=%s phone=%s. "
                    "Status=%s, error=%s. Retry in %.1f sec. Attempt %s/%s",
                    lead.source_id,
                    lead.phone,
                    response.status_code,
                    api_error,
                    delay,
                    attempt + 1,
                    BITRIX_MAX_RETRIES,
                )
                time.sleep(delay)
                continue

            logger.error(
                "Bitrix24 create failed for source_id=%s phone=%s. Status=%s, error=%s",
                lead.source_id,
                lead.phone,
                response.status_code,
                api_error,
            )
            return None

    except Exception as error:
        logger.exception(
            "Unexpected error while sending source_id=%s to Bitrix24: %s",
            lead.source_id,
            error,
        )
        return None


def validate_lead_record(lead: LeadRecord) -> Optional[str]:
    if not lead.source_id:
        return "empty source_id"
    if not lead.phone:
        return "empty phone"
    if not lead.sheet_name:
        return "empty sheet_name"
    if lead.sheet_row <= 1:
        return f"invalid sheet_row={lead.sheet_row}"
    return None


def main() -> None:
    load_dotenv()

    webhook_url = get_env_required("BITRIX_WEBHOOK_URL")
    spreadsheet_id = get_env_required("GOOGLE_SHEET_ID")
    credentials_file = get_env_required("GOOGLE_CREDENTIALS_FILE")

    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database file not found: {DB_PATH}")

    service = create_sheets_service(credentials_file)
    status_column_cache: Dict[str, int] = {}

    conn = sqlite3.connect(DB_PATH)
    try:
        leads = fetch_pending_leads(conn)
        if not leads:
            logger.info("No leads with empty Статус_Б24 found in database")
            print("Нет лидов с пустым Статус_Б24 для загрузки.")
            return

        logger.info("Found %s pending leads in database", len(leads))

        total = len(leads)
        success = 0
        skipped = 0
        failed = 0

        for index, lead in enumerate(leads, start=1):
            validation_error = validate_lead_record(lead)
            if validation_error:
                skipped += 1
                logger.error(
                    "Skip source_id=%s because %s",
                    lead.source_id or "<empty>",
                    validation_error,
                )
                continue

            print(f"[{index}/{total}] Загружаем lead source_id={lead.source_id}, phone={lead.phone}")
            lead_url = send_to_bitrix24(lead, webhook_url)
            if not lead_url:
                failed += 1
                continue

            update_db_status(conn, lead.source_id, lead_url)

            try:
                status_column_index = get_status_column_index(
                    service,
                    spreadsheet_id,
                    lead.sheet_name,
                    status_column_cache,
                )
                update_sheet_status(
                    service,
                    spreadsheet_id,
                    lead.sheet_name,
                    lead.sheet_row,
                    status_column_index,
                    lead_url,
                )
            except Exception as error:
                failed += 1
                logger.exception(
                    "Lead source_id=%s created in Bitrix24 but Google Sheets update failed: %s",
                    lead.source_id,
                    error,
                )
                print(
                    f"Лид создан, но Google Sheets не обновлен для source_id={lead.source_id}: {error}"
                )
                time.sleep(0.5)
                continue

            success += 1
            print(lead_url)
            time.sleep(0.5)

        print(
            f"\nЗагрузка завершена. Успешно: {success}/{total}. "
            f"Ошибки: {failed}. Пропущено: {skipped}."
        )
        logger.info(
            "Upload completed. Total=%s, success=%s, failed=%s, skipped=%s",
            total,
            success,
            failed,
            skipped,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
