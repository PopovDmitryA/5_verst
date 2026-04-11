import re
import time
import random
import configparser
import pandas as pd
from tqdm import tqdm
from urllib.parse import urlparse, urljoin
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
from s95_http_client import S95HttpClient, S95BanDetected, S95TemporaryError, S95HttpError


# =========================
# Функция парсинга протокола
# =========================
def parse_protocol(page, name_point, date_event, client):
    parsed = urlparse(page)
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    soup = client.get_soup(
        page,
        allow_ban_html_check=True,
        sleep_before=True
    )

    # ---------- Протокол (бегуны) ----------
    protocol = soup.find('div', {'class': 'tab-pane', 'id': re.compile(r'^\w+')}) \
               or soup.find('div', {'class': 'tab-pane fade show active'}) \
               or soup

    rows = protocol.find_all('tr') if protocol else []
    runner_rows = []

    for row in rows:
        cols = row.find_all('td')
        if not cols:
            continue

        # Позиция
        position = cols[0].get_text(strip=True) if len(cols) > 0 else None

        # Имя + ссылка
        if len(cols) > 1:
            link_tag = cols[1].find('a', class_='athlete-link') or cols[1].find('a')
            athlete_name = (link_tag.get_text(strip=True) if link_tag else cols[1].get_text(strip=True) or None)
            athlete_href = urljoin(base_url, link_tag['href']) if (link_tag and link_tag.has_attr('href')) else None
        else:
            athlete_name, athlete_href = None, None

        # Время финиша
        if len(cols) > 2:
            time_cell = cols[2]
            for icon in time_cell.find_all(['i', 'span']):
                icon.decompose()
            race_time = time_cell.get_text(strip=True) or None
        else:
            race_time = None

        # Темп
        pace = cols[3].get_text(strip=True) if len(cols) > 3 else None

        # Клуб
        if len(cols) > 4:
            club_tag = cols[4].find('a')
            club_name = club_tag.get_text(strip=True) if club_tag else cols[4].get_text(strip=True) or None
            club_href = urljoin(base_url, club_tag['href']) if (club_tag and club_tag.has_attr('href')) else None
        else:
            club_name, club_href = None, None

        # user_id
        user_id = None
        if athlete_href:
            match = re.search(r'/(\d+)$', athlete_href)
            if match:
                user_id = match.group(1)

        # status_runner
        if not athlete_href and (athlete_name in ['НЕИЗВЕСТНЫЙ', 'NEPOZNATO']):
            status_runner = 'unknown_runner'
        else:
            status_runner = 'active_runner'

        runner_rows.append({
            'position': position,
            'name_runner': athlete_name,
            'link_runner': athlete_href,
            'user_id': user_id,
            'finish_time': race_time,
            'pace': pace,
            'club_name': club_name,
            'link_club': club_href,
            'status_runner': status_runner
        })

    if not runner_rows:
        df_runner = pd.DataFrame(columns=[
            'position', 'name_runner', 'link_runner', 'user_id',
            'finish_time', 'pace', 'club_name', 'link_club', 'status_runner'
        ])
    else:
        df_runner = pd.DataFrame(runner_rows)

    # ---------- Волонтёры ----------
    volunteers_header = soup.find('h4', string=re.compile(r'Волонт[ёe]р|Volonteri', re.I))
    volunteers_table = volunteers_header.find_next('table') if volunteers_header else None

    vol_rows = []
    if volunteers_table:
        for row in volunteers_table.find_all('tr')[1:]:
            cols = row.find_all('td')
            if len(cols) < 2:
                continue

            athlete_link = cols[0].find('a')
            name = (athlete_link.get_text(strip=True) if athlete_link else cols[0].get_text(strip=True) or None)
            link = urljoin(base_url, athlete_link['href']) if (athlete_link and athlete_link.has_attr('href')) else None

            # user_id
            user_id = None
            if link:
                match = re.search(r'/(\d+)$', link)
                if match:
                    user_id = match.group(1)

            role = cols[1].get_text(strip=True) if len(cols) > 1 else None
            vol_rows.append({
                'name_runner': name,
                'link_runner': link,
                'user_id': user_id,
                'vol_role': role
            })

    if not vol_rows:
        df_vol = pd.DataFrame(columns=['name_runner', 'link_runner', 'user_id', 'vol_role'])
    else:
        df_vol = pd.DataFrame(vol_rows)

    # Добавляем name_point и date_event
    df_runner['name_point'] = name_point
    df_runner['date_event'] = date_event
    df_vol['name_point'] = name_point
    df_vol['date_event'] = date_event

    # Приведение типов
    df_runner['position'] = pd.to_numeric(df_runner.get('position'), errors='coerce').astype('Int64')
    for col in ['finish_time', 'pace']:
        df_runner[col] = df_runner[col].replace('', pd.NA)
        df_runner[col] = pd.to_datetime(df_runner[col], format='%H:%M:%S', errors='coerce')
        df_runner[col] = df_runner[col].dt.time

    df_runner['user_id'] = df_runner['user_id'].astype('string')
    df_vol['user_id'] = df_vol['user_id'].astype('string')

    return df_runner, df_vol

def log(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def sleep_range(a: float, b: float, reason: str):
    sec = random.uniform(a, b)
    log(f"Сон {sec:.0f}s ({reason})")
    time.sleep(sec)
# =========================
# Основной скрипт
# =========================
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
        pool_pre_ping=True,  # проверяет соединение перед использованием
        pool_recycle=900,  # раз в 15 минут пересоздает соединения
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

    # Получаем список протоколов, которые ещё не загружены
    query = """
    WITH missing_events AS (
        SELECT
            e.link_event,
            e.date_event,
            e.name_point,
            ROW_NUMBER() OVER (
                PARTITION BY e.name_point
                ORDER BY e.date_event
            ) AS rn
        FROM s95_list_all_events e
        WHERE NOT EXISTS (
            SELECT 1
            FROM s95_details_protocol p
            WHERE p.name_point = e.name_point
              AND p.date_event = e.date_event
        )
    )
    SELECT
        link_event,
        date_event,
        name_point
    FROM missing_events
    ORDER BY rn, date_event, name_point
    """
    events = pd.read_sql(query, engine)

    # ====== SLOW MODE (как ты хочешь) ======
    MIN_SLEEP_BETWEEN_EVENTS = 120  # минимум 2 минуты между событиями
    MAX_SLEEP_BETWEEN_EVENTS = 300  # максимум 5 минут между событиями

    SESSION_RESET_MIN = 8
    SESSION_RESET_MAX = 15

    next_reset_at = random.randint(SESSION_RESET_MIN, SESSION_RESET_MAX)
    processed = 0

    BATCH_SIZE = 10  # каждые 10 протоколов
    BATCH_SLEEP = 600  # 10 минут

    BAN_STRIKES_LIMIT = 3  # после 2-3 банов завершаемся
    ban_strikes = 0  # счётчик банов
    TEMP_ERROR_SLEEP = 150

    for i, row in tqdm(events.iterrows(), total=len(events), desc="Обработка протоколов"):
        success = False

        try:
            df_runner, df_vol = parse_protocol(
                row['link_event'],
                row['name_point'],
                row['date_event'],
                client
            )

            with engine.begin() as conn:
                if not df_runner.empty:
                    df_runner.to_sql('s95_details_protocol', conn, if_exists='append', index=False)
                if not df_vol.empty:
                    df_vol.to_sql('s95_details_vol', conn, if_exists='append', index=False)

            sleep_range(MIN_SLEEP_BETWEEN_EVENTS, MAX_SLEEP_BETWEEN_EVENTS, "после успеха")
            success = True

        except S95BanDetected as e:
            ban_strikes += 1
            log(f"BAN #{ban_strikes}: {e}")
            if ban_strikes >= BAN_STRIKES_LIMIT:
                log("Достигнут лимит банов — завершаю скрипт.")
                raise SystemExit(2)
            log("Ban-like signal получен, прерываем текущий прогон.")

        except S95TemporaryError as e:
            log(f"Временная сетевая ошибка для {row['link_event']}: {e}")
            time.sleep(TEMP_ERROR_SLEEP)

        except S95HttpError as e:
            log(f"HTTP ошибка для {row['link_event']}: {e}")

        except SQLAlchemyError as e:
            print(f"DB ошибка для {row['link_event']}: {e}. Пересоздаю engine и жду 60s.")
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
            print(f"Неожиданная ошибка {row['link_event']}: {e}")

        if not success:
            sleep_range(MIN_SLEEP_BETWEEN_EVENTS, MAX_SLEEP_BETWEEN_EVENTS, "после неуспеха")
            log(f"Пропускаем протокол после неуспешной обработки: {row['link_event']}")
            continue

        if (i + 1) % BATCH_SIZE == 0:
            log(f"Пакет из {BATCH_SIZE} протоколов обработан, сон {BATCH_SLEEP}s")
            time.sleep(BATCH_SLEEP)

        processed += 1

        if processed >= next_reset_at:
            client.reset_session()
            time.sleep(random.uniform(10, 30))
            processed = 0
            next_reset_at = random.randint(SESSION_RESET_MIN, SESSION_RESET_MAX)
