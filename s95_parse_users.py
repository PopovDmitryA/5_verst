import sys
import time
import random
import argparse
import configparser
from typing import Optional, Dict, Any

import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path

from s95_http_client import S95HttpClient, S95BanDetected, S95TemporaryError, S95HttpError
from telegram_notifier import send_telegram_notification, escape_markdown


def parse_runner_page(link_s95_runner, client):
    """
    Парсинг страницы участника.
    Возвращает s95_barcode и planning.
    """
    soup = client.get_soup(
        link_s95_runner,
        allow_ban_html_check=True,
        sleep_before=True
    )

    barcode_element = soup.find('h5', {'id': 'barcodeModalLabel'})
    s95_barcode = barcode_element.text.strip() if barcode_element else None

    parse_plan = soup.find('div', {'class': 'badge bg-success mb-2'})
    if parse_plan:
        text_value = parse_plan.get_text(strip=True)
        planning = text_value.replace('Собирается в ', '').strip() if text_value.startswith('Собирается в ') else None
    else:
        planning = None

    return s95_barcode, planning


def log(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def sleep_range(a: float, b: float, reason: str):
    sec = random.uniform(a, b)
    log(f"Сон {sec:.0f}s ({reason})")
    time.sleep(sec)


def pre_parse_jitter():
    sec = random.uniform(5, 10)
    log(f"Короткая пауза перед стартом парсинга: {sec:.1f}s")
    time.sleep(sec)


def safe_tg_send(message: str):
    try:
        send_telegram_notification(message)
    except Exception as e:
        log(f"Не удалось отправить уведомление в Telegram: {e}")


def get_available_count(engine) -> int:
    query = """
        WITH combined_ids AS (
            SELECT user_id::text AS s95_id
            FROM s95_details_protocol
            WHERE user_id IS NOT NULL
            UNION
            SELECT user_id::text AS s95_id
            FROM s95_details_vol
            WHERE user_id IS NOT NULL
        )
        SELECT COUNT(*) AS cnt
        FROM combined_ids c
        LEFT JOIN s95_runners r ON r.s95_id = c.s95_id
        WHERE r.s95_id IS NULL
    """
    df = pd.read_sql(query, engine)
    return int(df.iloc[0]["cnt"])


def resolve_limit(available_count: int, cli_limit: Optional[int]) -> int:
    if available_count <= 0:
        return 0

    if cli_limit is not None:
        if cli_limit <= 0:
            return available_count
        return min(cli_limit, available_count)

    raw_value = input(
        f"Доступно участников для парсинга: {available_count}\n"
        f"Сколько записей обработать? 0 = все: "
    ).strip()

    if not raw_value:
        requested = available_count
    else:
        requested = int(raw_value)

    if requested <= 0:
        return available_count

    return min(requested, available_count)

def get_random_runner(engine) -> Optional[Dict[str, Any]]:
    """
    Каждый раз берём одного случайного участника, которого ещё нет в s95_runners.
    Это снижает шанс пересечения при параллельном запуске на сервере и локально.
    """
    query = """
        WITH combined_ids AS (
            SELECT user_id::text AS s95_id
            FROM s95_details_protocol
            WHERE user_id IS NOT NULL
            UNION
            SELECT user_id::text AS s95_id
            FROM s95_details_vol
            WHERE user_id IS NOT NULL
        )
        SELECT
            c.s95_id,
            'https://s95.ru/athletes/' || c.s95_id AS link_s95_runner
        FROM combined_ids c
        LEFT JOIN s95_runners r ON r.s95_id = c.s95_id
        WHERE r.s95_id IS NULL
        ORDER BY random()
        LIMIT 1
    """
    df = pd.read_sql(query, engine)
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def insert_runner(conn, s95_id: str, link_s95_runner: str, s95_barcode: Optional[str], planning: Optional[str]) -> bool:
    """
    Вставка участника с защитой от гонки.
    Если другой процесс успел вставить запись раньше, ON CONFLICT DO NOTHING
    просто вернёт 0 строк и скрипт пойдёт дальше без падения.

    ВАЖНО: нужен UNIQUE или PK на s95_runners(s95_id).
    """
    query = text("""
        INSERT INTO s95_runners (s95_id, link_s95_runner, s95_barcode, planning)
        VALUES (:s95_id, :link_s95_runner, :s95_barcode, :planning)
        ON CONFLICT (s95_id) DO NOTHING
    """)
    result = conn.execute(query, {
        "s95_id": s95_id,
        "link_s95_runner": link_s95_runner,
        "s95_barcode": s95_barcode,
        "planning": planning
    })
    return result.rowcount > 0


def format_start_message(total_available: int, selected_count: int, launch_mode: str, started_at_text: str) -> str:
    return (
        f"*__🔵 {SCRIPT_TG_NAME}__*\n\n"
        f"*Время запуска:* {escape_markdown(started_at_text)}\n"
        f"*Статус:* запуск\n"
        f"*Режим запуска:* {escape_markdown(launch_mode)}\n"
        f"*Доступно к обновлению:* {total_available}\n"
        f"*Взято в работу:* {selected_count}\n"
        f"*Источник данных:* {escape_markdown('s95_details_protocol + s95_details_vol -> s95_runners')}"
    )

def format_empty_message(total_available: int, launch_mode: str, started_at_text: str) -> str:
    return (
        f"*__⚪ {SCRIPT_TG_NAME}__*\n\n"
        f"*Время запуска:* {escape_markdown(started_at_text)}\n"
        f"*Статус:* запуск не требуется\n"
        f"*Режим запуска:* {escape_markdown(launch_mode)}\n"
        f"*Доступно к обновлению:* {total_available}\n"
        f"Обновлять нечего\\."
    )

def format_finish_message(
    launch_mode: str,
    planned_count: int,
    attempt_count: int,
    success_count: int,
    already_inserted_count: int,
    ban_count: int,
    temp_error_count: int,
    http_error_count: int,
    db_error_count: int,
    other_error_count: int,
    elapsed_seconds: float,
    stopped_by_ban: bool,
    started_at_text: str
) -> str:
    if stopped_by_ban:
        status_emoji = "🔴"
        status_text = "завершено досрочно из\\-за BAN"
    elif db_error_count > 0 or other_error_count > 0 or http_error_count > 0 or temp_error_count > 0:
        status_emoji = "🟠"
        status_text = "завершено с ошибками"
    elif success_count > 0 or already_inserted_count > 0:
        status_emoji = "🟢"
        status_text = "завершено успешно"
    else:
        status_emoji = "⚪"
        status_text = "без изменений"
    return (
        f"*__{status_emoji} {SCRIPT_TG_NAME}__*\n\n"
        f"*Время запуска:* {escape_markdown(started_at_text)}\n"
        f"*Статус:* {status_text}\n"
        f"*Режим запуска:* {escape_markdown(launch_mode)}\n"
        f"*План к обработке:* {planned_count}\n"
        f"*Попыток обработки:* {attempt_count}\n"
        f"*Успешно добавлено:* {success_count}\n"
        f"*Уже были добавлены другим процессом:* {already_inserted_count}\n"
        f"*BAN событий:* {ban_count}\n"
        f"*Временных ошибок:* {temp_error_count}\n"
        f"*HTTP ошибок:* {http_error_count}\n"
        f"*DB ошибок:* {db_error_count}\n"
        f"*Прочих ошибок:* {other_error_count}\n"
        f"*Длительность:* {escape_markdown(f'{elapsed_seconds / 60:.1f} мин')}"
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Парсинг новых участников s95_runners"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Сколько записей обработать. 0 = все доступные."
    )
    args = parser.parse_args()

    CURRENT_DIR = Path(__file__).resolve().parent
    PROJECT_ROOT = CURRENT_DIR.parent
    CONFIG_PATH = PROJECT_ROOT / "5_verst.ini"

    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)

    db_host = config['five_verst_stats']['host']
    db_user = config['five_verst_stats']['username']
    db_pass = config['five_verst_stats']['password']
    db_name = config['five_verst_stats']['dbname']

    credential = f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}'
    engine = create_engine(
        credential,
        pool_pre_ping=True,
        pool_recycle=900,
        pool_size=5,
        max_overflow=10,
    )

    client = S95HttpClient(
        connect_timeout=10,
        read_timeout=60,
        min_delay=1.5,
        max_delay=3.5,
        cooldown_seconds=1800,
        max_retries=2,
    )
    SCRIPT_TG_NAME = "s95\\_parse\\_users"
    launch_mode = "CLI/cron" if args.limit is not None else "interactive"

    started_at = time.time()
    started_at_text = time.strftime("%Y-%m-%d %H:%M:%S")

    total_available = get_available_count(engine)
    print(f"Доступно новых участников для парсинга: {total_available}")

    selected_count = resolve_limit(total_available, args.limit)

    if total_available == 0 or selected_count == 0:
        print("Новых участников для парсинга нет.")
        safe_tg_send(format_empty_message(total_available, launch_mode, started_at_text))
        raise SystemExit

    print(f"Будет обработано записей: {selected_count}")
    safe_tg_send(format_start_message(total_available, selected_count, launch_mode, started_at_text))

    MIN_SLEEP_BETWEEN_USERS = 120
    MAX_SLEEP_BETWEEN_USERS = 300

    SESSION_RESET_MIN = 8
    SESSION_RESET_MAX = 15

    next_reset_at = random.randint(SESSION_RESET_MIN, SESSION_RESET_MAX)
    processed_success_since_reset = 0

    BATCH_SIZE = 10
    BATCH_SLEEP = 600

    BAN_STRIKES_LIMIT = 3
    ban_strikes = 0
    TEMP_ERROR_SLEEP = 150

    attempt_count = 0
    success_count = 0
    already_inserted_count = 0
    temp_error_count = 0
    http_error_count = 0
    db_error_count = 0
    other_error_count = 0
    stopped_by_ban = False

    progress_bar = tqdm(total=selected_count, desc="Обработка участников")

    try:
        while attempt_count < selected_count:
            row = get_random_runner(engine)

            if row is None:
                log("Доступных участников для обработки больше не осталось.")
                break

            success = False
            s95_id = row['s95_id']
            link_s95_runner = row['link_s95_runner']

            log(f"Выбран участник: {s95_id} | {link_s95_runner}")

            pre_parse_jitter()
            attempt_count += 1
            is_last_iteration = attempt_count >= selected_count

            try:
                s95_barcode, planning = parse_runner_page(link_s95_runner, client)

                with engine.begin() as conn:
                    inserted = insert_runner(
                        conn=conn,
                        s95_id=s95_id,
                        link_s95_runner=link_s95_runner,
                        s95_barcode=s95_barcode,
                        planning=planning
                    )

                if inserted:
                    success = True
                    success_count += 1
                    progress_bar.update(1)
                    if not is_last_iteration:
                        sleep_range(MIN_SLEEP_BETWEEN_USERS, MAX_SLEEP_BETWEEN_USERS, "после успеха")
                else:
                    already_inserted_count += 1
                    progress_bar.update(1)
                    log(f"Пропуск {s95_id}: запись уже была добавлена другим процессом.")
                    if not is_last_iteration:
                        sleep_range(MIN_SLEEP_BETWEEN_USERS, MAX_SLEEP_BETWEEN_USERS, "после пересечения")


            except S95BanDetected as e:

                ban_strikes += 1

                log(f"BAN #{ban_strikes} для {link_s95_runner}: {e}")

                if ban_strikes >= BAN_STRIKES_LIMIT:
                    log("Достигнут лимит банов — завершаю скрипт.")

                    stopped_by_ban = True

                    break

                log("Ban-like signal получен, продолжаем с новой попыткой после паузы.")

                if not is_last_iteration:
                    sleep_range(MIN_SLEEP_BETWEEN_USERS, MAX_SLEEP_BETWEEN_USERS, "после BAN сигнала")


            except S95TemporaryError as e:

                temp_error_count += 1

                log(f"Временная сетевая ошибка для {link_s95_runner}: {e}")

                time.sleep(TEMP_ERROR_SLEEP)

                if not is_last_iteration:
                    sleep_range(MIN_SLEEP_BETWEEN_USERS, MAX_SLEEP_BETWEEN_USERS, "после неуспеха")


            except S95HttpError as e:

                http_error_count += 1

                log(f"HTTP ошибка для {link_s95_runner}: {e}")

                if not is_last_iteration:
                    sleep_range(MIN_SLEEP_BETWEEN_USERS, MAX_SLEEP_BETWEEN_USERS, "после неуспеха")


            except SQLAlchemyError as e:

                db_error_count += 1

                log(f"DB ошибка для {link_s95_runner}: {e}. Пересоздаю engine и жду 60s.")

                try:

                    engine.dispose()

                except Exception:

                    pass

                engine = create_engine(

                    credential,

                    pool_pre_ping=True,

                    pool_recycle=900,

                    pool_size=5,

                    max_overflow=10,

                )

                time.sleep(60)

                if not is_last_iteration:
                    sleep_range(MIN_SLEEP_BETWEEN_USERS, MAX_SLEEP_BETWEEN_USERS, "после DB ошибки")


            except Exception as e:

                other_error_count += 1

                log(f"Неожиданная ошибка для {link_s95_runner}: {e}")

                if not is_last_iteration:
                    sleep_range(MIN_SLEEP_BETWEEN_USERS, MAX_SLEEP_BETWEEN_USERS, "после неуспеха")

            if success:
                processed_success_since_reset += 1

                if not is_last_iteration and success_count % BATCH_SIZE == 0:
                    log(f"Пакет из {BATCH_SIZE} успешных участников обработан, сон {BATCH_SLEEP}s")
                    time.sleep(BATCH_SLEEP)

                if not is_last_iteration and processed_success_since_reset >= next_reset_at:
                    client.reset_session()
                    time.sleep(random.uniform(10, 30))
                    processed_success_since_reset = 0
                    next_reset_at = random.randint(SESSION_RESET_MIN, SESSION_RESET_MAX)

    except Exception as e:
        elapsed_seconds = time.time() - started_at
        safe_tg_send(
            f"*__🔴 {SCRIPT_TG_NAME}__*\n\n"
            f"*Статус:* аварийное завершение\n"
            f"*Ошибка:* {escape_markdown(str(e))}\n"
            f"*Попыток обработки:* {attempt_count}\n"
            f"*Успешно добавлено:* {success_count}\n"
            f"*Уже были добавлены другим процессом:* {already_inserted_count}\n"
            f"*Длительность:* {escape_markdown(f'{elapsed_seconds / 60:.1f} мин')}"
        )
        raise
    finally:
        progress_bar.close()

    elapsed_seconds = time.time() - started_at

    finish_message = format_finish_message(
        launch_mode=launch_mode,
        planned_count=selected_count,
        attempt_count=attempt_count,
        success_count=success_count,
        already_inserted_count=already_inserted_count,
        ban_count=ban_strikes,
        temp_error_count=temp_error_count,
        http_error_count=http_error_count,
        db_error_count=db_error_count,
        other_error_count=other_error_count,
        elapsed_seconds=elapsed_seconds,
        stopped_by_ban=stopped_by_ban,
        started_at_text=started_at_text
    )

    print("\n" + finish_message)
    safe_tg_send(finish_message)