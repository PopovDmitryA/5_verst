import time
import random
import requests
import configparser
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
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
        text = parse_plan.get_text(strip=True)
        planning = text.replace('Собирается в ', '').strip() if text.startswith('Собирается в ') else None
    else:
        planning = None

    return s95_barcode, planning


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
    engine = create_engine(credential)
    client = S95HttpClient(
        connect_timeout=10,
        read_timeout=20,
        min_delay=1.0,
        max_delay=3.0,
        cooldown_seconds=1200,
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
        exit()

    for i, row in tqdm(runners.iterrows(), total=len(runners), desc="Обработка участников"):
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


        except S95BanDetected as e:
            print(f"⛔ BAN signal для {link_s95_runner}: {e}")
            break

        except S95TemporaryError as e:
            print(f"⚠️ Временная сетевая ошибка для {link_s95_runner}: {e}")
            continue

        except S95HttpError as e:
            print(f"❌ HTTP ошибка для {link_s95_runner}: {e}")
            continue

        except Exception as e:
            print(f"❌ Ошибка при обработке {link_s95_runner}: {e}")
            continue

        # Рандомная пауза каждые 5–15 участников
        if (i + 1) % random.randint(1, 5) == 0:
            time.sleep(random.randint(10, 20))
