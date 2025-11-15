import re
import time
import random
import requests
import configparser
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path

# =========================
# Функция парсинга протокола
# =========================
def parse_protocol(page, name_point, date_event):
    parsed = urlparse(page)
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    resp = requests.get(page, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')

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
    engine = create_engine(credential)

    # Получаем список протоколов, которые ещё не загружены
    query = """
        SELECT e.link_event, e.date_event, e.name_point
        FROM s95_list_all_events e
        WHERE NOT EXISTS (
            SELECT 1 FROM s95_details_protocol p
            WHERE p.name_point = e.name_point AND p.date_event = e.date_event
        )
        ORDER BY e.date_event
    """
    events = pd.read_sql(query, engine)

    consecutive_429 = 0
    for i, row in tqdm(events.iterrows(), total=len(events), desc="Обработка протоколов"):
        try:
            df_runner, df_vol = parse_protocol(row['link_event'], row['name_point'], row['date_event'])

            with engine.begin() as conn:
                if not df_runner.empty:
                    df_runner.to_sql('s95_details_protocol', conn, if_exists='append', index=False)
                if not df_vol.empty:
                    df_vol.to_sql('s95_details_vol', conn, if_exists='append', index=False)

            consecutive_429 = 0  # сброс счётчика при успехе

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                consecutive_429 += 1
                print(f"Ошибка 429 для {row['link_event']} ({consecutive_429} подряд)")
                if consecutive_429 >= 10:
                    print("Достигнут лимит 10 подряд ошибок 429. Останавливаем скрипт.")
                    break
                time.sleep(random.randint(10, 20))
                continue
            else:
                print(f"Ошибка при обработке {row['link_event']}: {e}")
                continue
        except Exception as e:
            print(f"Ошибка при обработке {row['link_event']}: {e}")
            continue

        # Рандомная пауза каждые 5–10 протоколов
        if (i + 1) % random.randint(5, 15) == 0:
            time.sleep(random.randint(10, 20))
