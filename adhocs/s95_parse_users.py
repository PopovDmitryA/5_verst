import time
import random
import requests
import configparser
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from pathlib import Path


def parse_runner_page(link_s95_runner):
    """
    Парсинг страницы участника.
    Возвращает s95_barcode и planning.
    """
    resp = requests.get(link_s95_runner, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')

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

    consecutive_429 = 0

    for i, row in tqdm(runners.iterrows(), total=len(runners), desc="Обработка участников"):
        s95_id = row['s95_id']
        link_s95_runner = row['link_s95_runner']

        try:
            s95_barcode, planning = parse_runner_page(link_s95_runner)

            df_insert = pd.DataFrame([{
                's95_id': s95_id,
                'link_s95_runner': link_s95_runner,
                's95_barcode': s95_barcode,
                'planning': planning
            }])

            with engine.begin() as conn:
                df_insert.to_sql('s95_runners', conn, if_exists='append', index=False)

            consecutive_429 = 0  # сброс при успехе

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                consecutive_429 += 1
                print(f"⛔ Ошибка 429 (Too Many Requests) для {link_s95_runner} ({consecutive_429} подряд)")
                time.sleep(random.randint(10, 20))
                if consecutive_429 >= 10:
                    print("❌ Достигнут лимит 10 подряд ошибок 429. Останавливаем скрипт.")
                    break
                continue
            else:
                print(f"❌ HTTP ошибка {e.response.status_code} для {link_s95_runner}: {e}")
                continue

        except Exception as e:
            print(f"❌ Ошибка при обработке {link_s95_runner}: {e}")
            continue

        # Рандомная пауза каждые 5–15 участников
        if (i + 1) % random.randint(1, 5) == 0:
            time.sleep(random.randint(10, 20))
