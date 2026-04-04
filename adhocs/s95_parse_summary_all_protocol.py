import configparser
from sqlalchemy import create_engine
#from list_protocol_location_module import list_protocol_location  # импортируем твою функцию
import traceback

import requests
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from datetime import datetime

def parse_time_string(time_str):
    """Преобразуем строку вида '20:07' или '1:02:30' в формат hh:mm:ss"""
    if not time_str:
        return None
    parts = time_str.strip().split(':')
    if len(parts) == 2:  # mm:ss
        return f"00:{int(parts[0]):02d}:{int(parts[1]):02d}"
    elif len(parts) == 3:  # hh:mm:ss
        return f"{int(parts[0]):02d}:{int(parts[1]):02d}:{int(parts[2]):02d}"
    return None

def list_protocol_location(page):
    """Парсим страницу локации и приводим данные к нужным типам"""
    parsed = urlparse(page)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    t = requests.get(page)
    soup = BeautifulSoup(t.text, 'html.parser')

    table_all_events = soup.find('div', {'class': 'row row-cols-1'})
    rows = table_all_events.find_all('tr')

    data = []

    # Получаем заголовки
    header_row = rows[0]
    columns = [col.text.strip() for col in header_row.find_all(['td', 'th'])]
    columns.append('link_event')  # создаём колонку ссылки сразу с нужным именем

    # Словарь для переименования, учитываем разные языки
    rename_dict = {
        '#': 'index_event',
        'Дата': 'date_event', 'Datum': 'date_event',
        'Участники': 'count_runners', 'Sportisti': 'count_runners',
        'Волонтёры': 'count_vol', 'Volonteri': 'count_vol',
        'Первый': 'first_man', 'Prvi čovek': 'first_man',
        'Первая': 'first_woman', 'Prva žena': 'first_woman',
    }

    for row in rows[1:]:
        cols = [col.text.strip() for col in row.find_all('td')]

        # Получаем ссылку на мероприятие
        date_cell = row.find('td', class_='date')
        if date_cell and date_cell.find('a'):
            event_link = base_url + date_cell.find('a')['href']
        else:
            event_link = None

        cols.append(event_link)
        data.append(cols)

    df = pd.DataFrame(data, columns=columns)

    # Переименовываем колонки по словарю
    df = df.rename(columns={k: v for k, v in rename_dict.items() if k in df.columns})

    # Разбираем колонку "first_man" и "first_woman"
    first_man_list = []
    best_time_man_list = []
    first_woman_list = []
    best_time_woman_list = []

    for idx, row in df.iterrows():
        man_text = row.get('first_man')
        woman_text = row.get('first_woman')

        # Мужчина
        if man_text and '(' in man_text and ')' in man_text:
            name, time = man_text.rsplit('(', 1)
            first_man_list.append(name.strip())
            best_time_man_list.append(parse_time_string(time.replace(')', '').strip()))
        elif man_text:
            first_man_list.append(man_text.strip())
            best_time_man_list.append(None)
        else:
            first_man_list.append(None)
            best_time_man_list.append(None)

        # Женщина
        if woman_text and '(' in woman_text and ')' in woman_text:
            name, time = woman_text.rsplit('(', 1)
            first_woman_list.append(name.strip())
            best_time_woman_list.append(parse_time_string(time.replace(')', '').strip()))
        elif woman_text:
            first_woman_list.append(woman_text.strip())
            best_time_woman_list.append(None)
        else:
            first_woman_list.append(None)
            best_time_woman_list.append(None)

    df['first_man'] = first_man_list
    df['best_time_man'] = best_time_man_list
    df['first_woman'] = first_woman_list
    df['best_time_woman'] = best_time_woman_list

    # --- Приведение типов (как в твоём рабочем скрипте) ---
    for col in ['best_time_man', 'best_time_woman']:
        df[col] = df[col].replace('', pd.NA).fillna('00:00:00')
        df[col] = pd.to_datetime(df[col], format='%H:%M:%S', errors='coerce').dt.time

    for col in ['index_event', 'count_runners', 'count_vol']:
        df[col] = pd.to_numeric(df.get(col), errors='coerce').astype('Int64')

    df['date_event'] = pd.to_datetime(df.get('date_event'), format='%d.%m.%Y', errors='coerce')
    df['link_event'] = df['link_event'].astype('string')
    df['first_man'] = df['first_man'].astype('string')
    df['first_woman'] = df['first_woman'].astype('string')

    # Убираем старые колонки, если остались
    for col in ['Первый', 'Prvi čovek', 'Первая', 'Prva žena']:
        if col in df.columns:
            df = df.drop(columns=[col])

    return df


# --- Подключение к БД ---
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

# --- Получаем список локаций ---
query_locations = "SELECT name_point, link_point FROM s95_location;"
locations_df = pd.read_sql(query_locations, engine)

all_events_list = []

# --- Проходим по каждой ссылке ---
for idx, row in locations_df.iterrows():
    name_point = row['name_point']
    link_point = row['link_point']

    print(f"Обрабатываем локацию: {name_point} | {link_point}")

    try:
        df_events = list_protocol_location(link_point)
        df_events['name_point'] = name_point  # добавляем колонку с названием локации
        all_events_list.append(df_events)
        print(f"Успешно обработано: {name_point} ({len(df_events)} событий)")
    except Exception as e:
        print(f"\nОшибка при обработке локации: {name_point}")
        print(f"Ссылка: {link_point}")
        print(f"Тип ошибки: {type(e).__name__}")
        print(f"Сообщение: {str(e)}")
        print("Traceback:")
        traceback.print_exc()
        print("\nПродолжаем со следующей локацией...\n")

# --- Объединяем все данные ---
if all_events_list:
    all_events_df = pd.concat(all_events_list, ignore_index=True)
else:
    all_events_df = pd.DataFrame()  # на случай, если ничего не удалось собрать

# --- Записываем в таблицу ---
if not all_events_df.empty:
    all_events_df.to_sql(
        's95_list_all_events',
        engine,
        if_exists='replace',  # можно заменить на 'append', если таблица уже есть
        index=False
    )
    print("Данные успешно записаны в s95_list_all_events")
else:
    print("Нет данных для записи")