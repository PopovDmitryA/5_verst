import configparser
from sqlalchemy import create_engine, text
from s95_http_client import S95HttpClient, S95BanDetected, S95TemporaryError, S95HttpError
import traceback
import random
import time
from pathlib import Path
import pandas as pd
from urllib.parse import urlparse


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


def list_protocol_location(page, client):
    """Парсим страницу локации и приводим данные к нужным типам"""
    parsed = urlparse(page)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    soup = client.get_soup(
        page,
        allow_ban_html_check=True,
        sleep_before=True
    )

    table_all_events = soup.find('div', {'class': 'row row-cols-1'})
    if table_all_events is None:
        raise ValueError(f"Не найден блок с таблицей событий: {page}")
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
            name, time_str = man_text.rsplit('(', 1)
            first_man_list.append(name.strip())
            best_time_man_list.append(parse_time_string(time_str.replace(')', '').strip()))
        elif man_text:
            first_man_list.append(man_text.strip())
            best_time_man_list.append(None)
        else:
            first_man_list.append(None)
            best_time_man_list.append(None)

        # Женщина
        if woman_text and '(' in woman_text and ')' in woman_text:
            name, time_str = woman_text.rsplit('(', 1)
            first_woman_list.append(name.strip())
            best_time_woman_list.append(parse_time_string(time_str.replace(')', '').strip()))
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
        if col in df.columns:
            df[col] = pd.to_numeric(df.get(col), errors='coerce').astype('Int64')

    if 'date_event' in df.columns:
        df['date_event'] = pd.to_datetime(df.get('date_event'), format='%d.%m.%Y', errors='coerce')

    if 'link_event' in df.columns:
        df['link_event'] = df['link_event'].astype('string')

    df['first_man'] = df['first_man'].astype('string')
    df['first_woman'] = df['first_woman'].astype('string')

    # Убираем старые колонки, если остались
    for col in ['Первый', 'Prvi čovek', 'Первая', 'Prva žena']:
        if col in df.columns:
            df = df.drop(columns=[col])

    return df


def save_summary_and_mark_checked(df_events, link_point, conn):
    """
    В одной транзакции:
    1. добавляет только новые строки в s95_list_all_events
    2. обновляет last_summary_checked_at в s95_location
    """
    if df_events.empty:
        update_query = """
            UPDATE s95_location
            SET last_summary_checked_at = NOW()
            WHERE link_point = :link_point
        """
        conn.execute(text(update_query), {"link_point": link_point})
        return 0

    df_events = df_events.drop_duplicates(subset=['name_point', 'date_event']).copy()
    df_events['date_event'] = pd.to_datetime(df_events['date_event'])

    existing_keys_query = """
        SELECT name_point, date_event
        FROM s95_list_all_events
    """
    existing_keys_df = pd.read_sql(existing_keys_query, conn)

    if not existing_keys_df.empty:
        existing_keys_df['date_event'] = pd.to_datetime(existing_keys_df['date_event'])

        new_rows_df = df_events.merge(
            existing_keys_df,
            on=['name_point', 'date_event'],
            how='left',
            indicator=True
        )
        new_rows_df = new_rows_df[new_rows_df['_merge'] == 'left_only'].drop(columns=['_merge'])
    else:
        new_rows_df = df_events.copy()

    if not new_rows_df.empty:
        new_rows_df.to_sql(
            's95_list_all_events',
            conn,
            if_exists='append',
            index=False
        )

    # ВАЖНО: Обновляем last_summary_checked_at ДЛЯ ВСЕХ ОБРАБОТАННЫХ ЛОКАЦИЙ
    # Даже если не добавили новых записей, нужно обновить время проверки
    update_query = """
        UPDATE s95_location
        SET last_summary_checked_at = NOW()
        WHERE link_point = :link_point
    """
    conn.execute(text(update_query), {"link_point": link_point})

    return len(new_rows_df)


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

client = S95HttpClient(
    connect_timeout=10,
    read_timeout=45,
    min_delay=18.0,
    max_delay=37.5,
    cooldown_seconds=1200,
    max_retries=2,
)

MIN_SLEEP_BETWEEN_LOCATIONS = 90
MAX_SLEEP_BETWEEN_LOCATIONS = 180

# --- Получаем список локаций ---
raw_value = input("Сколько парков проверить? 0 = все: ").strip()
parks_to_check = int(raw_value) if raw_value else 0

base_query_locations = """
    SELECT name_point, link_point, last_summary_checked_at
    FROM s95_location
    WHERE link_point IS NOT NULL and is_pause is not true
    ORDER BY last_summary_checked_at NULLS FIRST, name_point
"""

if parks_to_check == 0:
    locations_df = pd.read_sql(base_query_locations, engine)
else:
    query_locations = base_query_locations + f" LIMIT {parks_to_check}"
    locations_df = pd.read_sql(query_locations, engine)

# Показываем список локаций для проверки
print(f"\nНайдено локаций для обработки: {len(locations_df)}")
for i, row in locations_df.iterrows():
    last_checked = row['last_summary_checked_at']
    last_checked_str = last_checked.strftime('%Y-%m-%d %H:%M:%S') if last_checked else 'никогда'
    print(f"  {i + 1}. {row['name_point']} (последняя проверка: {last_checked_str})")
print()

# --- Проходим по каждой ссылке ---
SESSION_RESET_MIN = 4
SESSION_RESET_MAX = 8

processed_locations = 0
next_reset_at = random.randint(SESSION_RESET_MIN, SESSION_RESET_MAX)

for idx, row in locations_df.iterrows():
    name_point = row['name_point']
    link_point = row['link_point']

    print(f"\n{'=' * 60}")
    print(f"Обрабатываем локацию: {name_point} | {link_point}")
    print(f"{'=' * 60}")

    try:
        df_events = list_protocol_location(link_point, client)
        df_events['name_point'] = name_point

        with engine.begin() as conn:
            added_rows = save_summary_and_mark_checked(df_events, link_point, conn)

        print(f"✅ Успешно обработано: {name_point} ({len(df_events)} событий), добавлено новых: {added_rows}")

        processed_locations += 1

        # reset session через случайное число локаций
        if processed_locations >= next_reset_at:
            print(f"🔄 Reset session после {processed_locations} локаций")
            client.reset_session()
            time.sleep(random.uniform(10, 30))  # пауза после reset
            processed_locations = 0
            next_reset_at = random.randint(SESSION_RESET_MIN, SESSION_RESET_MAX)

        # Проверяем, не последняя ли это локация
        if idx < len(locations_df) - 1:
            sleep_seconds = random.uniform(MIN_SLEEP_BETWEEN_LOCATIONS, MAX_SLEEP_BETWEEN_LOCATIONS)
            print(f"😴 Пауза {sleep_seconds:.0f} секунд перед следующей локацией")
            time.sleep(sleep_seconds)
        else:
            print(f"\n🏁 Все {len(locations_df)} локаций обработаны!")

    except S95BanDetected as e:
        print(f"\n🚫 BAN сигнал при обработке локации: {name_point}")
        print(f"Ссылка: {link_point}")
        print(f"Сообщение: {e}")
        print("Останавливаем прогон.\n")
        break

    except S95TemporaryError as e:
        print(f"\n⚠️ Временная сетевая ошибка при обработке локации: {name_point}")
        print(f"Ссылка: {link_point}")
        print(f"Сообщение: {e}")
        print("Пропускаем и продолжаем.\n")
        continue

    except S95HttpError as e:
        print(f"\n❌ HTTP ошибка при обработке локации: {name_point}")
        print(f"Ссылка: {link_point}")
        print(f"Сообщение: {e}")
        print("Пропускаем и продолжаем.\n")
        continue

    except Exception as e:
        print(f"\n💥 Ошибка при обработке локации: {name_point}")
        print(f"Ссылка: {link_point}")
        print(f"Тип ошибки: {type(e).__name__}")
        print(f"Сообщение: {str(e)}")
        print("Traceback:")
        traceback.print_exc()
        print("\nПродолжаем со следующей локацией...\n")