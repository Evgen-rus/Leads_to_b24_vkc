# Leads_to_b24_vkc

Автоматизация загрузки лидов из Google Sheets в Bitrix24 через локальную SQLite-базу.

Основной рабочий контур состоит из двух скриптов:

- 1_save_gsheet_to_sqlite.py читает Google Sheets и сохраняет лиды в `lr182.db`
- 2_upload_sqlite_to_bitrix.py берет из `lr182.db` только лиды с пустым `Статус_Б24`, создает лиды в Bitrix24 и записывает ссылку обратно в Google Sheets и SQLite

## Как работает процесс

1. Скрипт 1_save_gsheet_to_sqlite.py подключается к Google Sheets по `service account`.
2. Из листа берутся колонки:
   `ID`, `Дата`, `Номера`, `Канал`, `Источник`, `Статус_Б24`.
3. В SQLite сохраняются только свежие записи за последние `3` дня.
4. Поле `Статус_Б24` из таблицы сохраняется в БД в колонку `bitrix24_info`.
5. Скрипт 2_upload_sqlite_to_bitrix.py выбирает только записи, где `bitrix24_info` пустой.
6. Для каждой такой записи создается лид в Bitrix24:
   `PHONE` берется из `phone`
   `COMMENTS` берется из `source_lead`
7. После успешного создания лида ссылка вида `https://.../crm/lead/details/<id>/` записывается:
   в Google Sheets в колонку `Статус_Б24`
   в SQLite в поле `bitrix24_info`

## Структура данных

Основная таблица SQLite:

- база: `lr182.db`
- таблица: `leads`

Ключевые поля:

- `source_id` уникальный ID строки/лида из Google Sheets
- `event_dt` дата лида
- `phone` телефон
- `channel` канал
- `source_lead` источник лида, используется как `COMMENTS` в Bitrix24
- `bitrix24_info` статус из Google Sheets или ссылка на созданный лид Bitrix24
- `sheet_name` имя вкладки Google Sheets
- `sheet_row` номер строки в Google Sheets
- `inserted_at` время вставки в БД, MSK

## Переменные окружения

Проект использует `.env`.

Обязательные переменные:

- `GOOGLE_CREDENTIALS_FILE`
- `GOOGLE_SHEET_ID`
- `GOOGLE_SHEET_NAME`
- `BITRIX_WEBHOOK_URL`

Опциональные:

- `BITRIX_MAX_RETRIES`
- `BITRIX_RETRY_BASE_DELAY`
- `BITRIX_SOURCE_ID`
- `BITRIX_STATUS_ID`
- `BITRIX_ASSIGNED_BY_ID`
- `BITRIX_ROISTAT`

Пример минимально нужных значений:

```env
GOOGLE_CREDENTIALS_FILE=credentials/sheets-data-bot-b8f4cc6634fc.json
GOOGLE_SHEET_ID=your_google_sheet_id
GOOGLE_SHEET_NAME=Данные
BITRIX_WEBHOOK_URL=https://your-portal/rest/1/your_webhook/
```

## Требования

- Ubuntu server
- Python 3.11+ желательно
- доступ к Google Sheets API
- `service account` должен иметь доступ к нужной Google таблице
- рабочий webhook Bitrix24 с правами на CRM

Установка зависимостей:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Ручной запуск

Сохранение лидов из Google Sheets в SQLite:

```bash
cd /opt/Leads_to_b24_vkc
/opt/Leads_to_b24_vkc/venv/bin/python 1_save_gsheet_to_sqlite.py
```

Загрузка лидов из SQLite в Bitrix24:

```bash
cd /opt/Leads_to_b24_vkc
/opt/Leads_to_b24_vkc/venv/bin/python 2_upload_sqlite_to_bitrix.py
```

## Cron на Ubuntu

После деплоя на сервер настроить расписание cron по московскому времени:

```bash
crontab -e
```

Для первого запуска после деплоя добавить вариант с отдельными `cron`-логами:

```cron
# ==== Leads_to_b24_vkc SCHEDULE ====
CRON_TZ=Europe/Moscow
33 8-17 * * * cd /opt/Leads_to_b24_vkc && /opt/Leads_to_b24_vkc/venv/bin/python 1_save_gsheet_to_sqlite.py >/dev/null 2>> /opt/Leads_to_b24_vkc/logs/cron_save_gsheet.log
35 8-17 * * * cd /opt/Leads_to_b24_vkc && /opt/Leads_to_b24_vkc/venv/bin/python 2_upload_sqlite_to_bitrix.py >/dev/null 2>> /opt/Leads_to_b24_vkc/logs/cron_upload_sqlite.log
```

Этот режим нужен на этапе проверки, чтобы поймать ошибки запуска `cron`:

- неверный путь к `python`
- ошибки `cd`
- проблемы с правами
- отсутствие `.env`
- отсутствие `credentials`
- падение скрипта до штатного логирования

После того как убедишься, что задачи стабильно запускаются на сервере, рекомендуется перейти на основной рабочий вариант без `cron`-логов, чтобы не было дублей:

```cron
# ==== Leads_to_b24_vkc SCHEDULE ====
CRON_TZ=Europe/Moscow
33 8-17 * * * cd /opt/Leads_to_b24_vkc && /opt/Leads_to_b24_vkc/venv/bin/python 1_save_gsheet_to_sqlite.py >/dev/null 2>/dev/null
35 8-17 * * * cd /opt/Leads_to_b24_vkc && /opt/Leads_to_b24_vkc/venv/bin/python 2_upload_sqlite_to_bitrix.py >/dev/null 2>/dev/null
```

Текущее расписание означает:

- в `08:33, 09:33, ..., 17:33 MSK` обновляем SQLite из Google Sheets
- в `08:35, 09:35, ..., 17:35 MSK` загружаем новые лиды из SQLite в Bitrix24

## Логи

Каждый основной скрипт пишет в свой лог-файл:

- `logs/1_save_gsheet_to_sqlite.log`
- `logs/2_upload_sqlite_to_bitrix.log`
- `logs/upload_leads.log`

Настройка логирования находится в setup.py

Особенности:

- ротация по дням
- время в логах московское
- хранение архивов `14` дней

Если используется проверочный вариант, `cron` stderr пишется отдельно:

- `logs/cron_save_gsheet.log`
- `logs/cron_upload_sqlite.log`

## Важные замечания

- 1_save_gsheet_to_sqlite.py использует окно выгрузки `DAYS_LOOKBACK = 3`
- 2_upload_sqlite_to_bitrix.py не отправляет повторно те лиды, у которых уже заполнен `bitrix24_info`
- если Bitrix24 недоступен или вернул ошибку после всех ретраев, запись остается незагруженной и будет обработана на следующем запуске
- если лид создан в Bitrix24, ссылка сохраняется в БД и в Google Sheets
