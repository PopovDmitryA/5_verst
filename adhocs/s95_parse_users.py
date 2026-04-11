import time
import random
import configparser
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
from s95_http_client import S95HttpClient, S95BanDetected, S95TemporaryError, S95HttpError


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

    # Ищем barcode
    barcode_element = soup.find('h5', {'id': 'barcodeModalLabel'})
    s95_barcode = barcode_element.text.strip() if barcode_element else None

    # Ищем план посещения (только русская версия)
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


if __name__ == "__main__":
    # Подключение к БД
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

    # Такие же защитные настройки, как в details_protocol
    client = S95HttpClient(
        connect_timeout=10,
        read_timeout=60,
        min_delay=1.5,
        max_delay=3.5,
        cooldown_seconds=1800,
        max_retries=2,
    )

    # Получаем список уникальных участников (user_id), которых нет в s95_runners
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
        ORDER BY c.s95_id::int
    """
    runners = pd.read_sql(query, engine)

    if runners.empty:
        print("Новых участников для парсинга нет.")
        raise SystemExit

    print(f"Найдено новых участников для парсинга: {len(runners)}")

    # ====== SLOW MODE как в details_protocol ======
    MIN_SLEEP_BETWEEN_USERS = 120   # минимум 2 минуты
    MAX_SLEEP_BETWEEN_USERS = 300   # максимум 5 минут

    SESSION_RESET_MIN = 8
    SESSION_RESET_MAX = 15

    next_reset_at = random.randint(SESSION_RESET_MIN, SESSION_RESET_MAX)
    processed = 0

    BATCH_SIZE = 10
    BATCH_SLEEP = 600  # 10 минут

    BAN_STRIKES_LIMIT = 3
    ban_strikes = 0
    TEMP_ERROR_SLEEP = 150

    for i, row in tqdm(runners.iterrows(), total=len(runners), desc="Обработка участников"):
        success = False

        s95_id = row['s95_id']
        link_s95_runner = row['link_s95_runner']

        try:
            s95_barcode, planning = parse_runner_page(link_s95_runner, client)

            df_insert = pd.DataFrame([{
                's95_id': s95_id,
                'link_s95_runner': link_s95_runner,
                's95_barcode': s95_barcode,
                'planning': planning
            }])

            with engine.begin() as conn:
                df_insert.to_sql('s95_runners', conn, if_exists='append', index=False)

            success = True
            sleep_range(MIN_SLEEP_BETWEEN_USERS, MAX_SLEEP_BETWEEN_USERS, "после успеха")

        except S95BanDetected as e:
            ban_strikes += 1
            log(f"BAN #{ban_strikes} для {link_s95_runner}: {e}")
            if ban_strikes >= BAN_STRIKES_LIMIT:
                log("Достигнут лимит банов — завершаю скрипт.")
                raise SystemExit(2)
            log("Ban-like signal получен, прерываем текущий прогон.")

        except S95TemporaryError as e:
            log(f"Временная сетевая ошибка для {link_s95_runner}: {e}")
            time.sleep(TEMP_ERROR_SLEEP)

        except S95HttpError as e:
            log(f"HTTP ошибка для {link_s95_runner}: {e}")

        except SQLAlchemyError as e:
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

        except Exception as e:
            log(f"Неожиданная ошибка для {link_s95_runner}: {e}")

        if not success:
            sleep_range(MIN_SLEEP_BETWEEN_USERS, MAX_SLEEP_BETWEEN_USERS, "после неуспеха")
            log(f"Пропускаем участника после неуспешной обработки: {link_s95_runner}")
            continue

        if (i + 1) % BATCH_SIZE == 0:
            log(f"Пакет из {BATCH_SIZE} участников обработан, сон {BATCH_SLEEP}s")
            time.sleep(BATCH_SLEEP)

        processed += 1

        if processed >= next_reset_at:
            client.reset_session()
            time.sleep(random.uniform(10, 30))
            processed = 0
            next_reset_at = random.randint(SESSION_RESET_MIN, SESSION_RESET_MAX)