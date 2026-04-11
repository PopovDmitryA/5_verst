import pandas as pd
import requests
from urllib.parse import urlparse, parse_qs, unquote
import time
import random
from sqlalchemy import create_engine, text
from pathlib import Path
import configparser
from s95_http_client import S95HttpClient

yandex_session = requests.Session()
yandex_session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
})
# --------------------- Конфиг и подключение к БД ---------------------
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

# --------------------- Парсинг ---------------------

def list_location(domen, client):
    """Парсим список локаций"""
    print(f"\n[INFO] Парсим список локаций для домена: {domen}")
    page = f"https://s95.{domen}/events"
    soup = client.get_soup(
        page,
        allow_ban_html_check=True,
        sleep_before=True
    )

    table_last_event = soup.find('div', {'class': 'row row-cols-1 row-cols-md-2 g-3'})
    if table_last_event is None:
        raise ValueError(f"Не найден блок со списком локаций: {page}")
    cards = table_last_event.find_all("div", class_="col")
    print(f"[INFO] Найдено {len(cards)} локаций в {domen}")

    data = []
    for card in cards:
        name = card.find("section", class_="fs-4").get_text(strip=True)
        a_tag = card.find("a", class_="stretched-link")
        full_name = a_tag.get_text(strip=True)
        link = f'https://s95.{domen}' + a_tag["href"]
        data.append([name, full_name, link])

    return pd.DataFrame(data, columns=["name_point", "full_name_point", "link_point"])

def collect_site_locations(client):
    """
    Собирает все локации с сайта только с лёгкими полями:
    name_point, full_name_point, link_point
    """
    df_all = pd.concat([
        list_location('ru', client),
        list_location('by', client),
        list_location('rs', client)
    ], ignore_index=True)

    df_all = df_all.drop_duplicates(subset=['link_point']).copy()

    print(f"\n[INFO] Всего найдено {len(df_all)} уникальных локаций по всем странам")
    return df_all

def load_existing_locations(engine):
    """
    Загружает существующие локации из БД.
    """
    query = """
        SELECT
            name_point,
            full_name_point,
            latitude,
            longitude,
            link_point
        FROM s95_location
        WHERE link_point IS NOT NULL and is_pause is not true
    """

    df = pd.read_sql(query, engine)
    if not df.empty:
        df = df.drop_duplicates(subset=['link_point']).copy()

    return df

def classify_location(site_row, db_row):
    """
    Возвращает одно из действий:
    - insert_new
    - update_light_only
    - deep_fill_missing
    - skip
    """
    if db_row is None:
        return "insert_new"

    light_changed = (
        (site_row['name_point'] != db_row['name_point']) or
        (site_row['full_name_point'] != db_row['full_name_point'])
    )

    deep_missing = (
        pd.isna(db_row['latitude']) or db_row['latitude'] is None or str(db_row['latitude']).strip() == "" or
        pd.isna(db_row['longitude']) or db_row['longitude'] is None or str(db_row['longitude']).strip() == ""
    )

    if deep_missing:
        return "deep_fill_missing"

    if light_changed:
        return "update_light_only"

    return "skip"

def get_yandex_map_link(event_url, client):
    """Получает ссылку на Яндекс.Карты из блока 'Наши контакты'"""
    print(f"    [INFO] Парсим страницу события: {event_url}")
    try:
        soup = client.get_soup(
            event_url,
            allow_ban_html_check=True,
            sleep_before=True
        )

        contact_titles = ['Наши контакты', 'Контакти', 'Нашы кантакты']

        contacts_block = None
        for title in contact_titles:
            contacts_block = soup.find('section', string=title)
            if contacts_block:
                break

        if not contacts_block:
            print(f"    [WARN] Блок 'Наши контакты' не найден")
            return None

        header_block = contacts_block.find_parent('div', class_='card-header')
        if header_block is None:
            print(f"    [WARN] Не найден card-header для блока контактов")
            return None

        card_body = header_block.find_next_sibling('div', class_='card-body')
        if card_body is None:
            print(f"    [WARN] Не найден card-body для блока контактов")
            return None

        for a in card_body.find_all('a', href=True):
            if a.get('title') and "Карта" in a.get('title'):
                print(f"    [OK] Найдена ссылка на карту: {a['href']}")
                return a['href']

        print(f"    [WARN] Ссылка на карту не найдена")
        return None
    except Exception as e:
        print(f"    [ERROR] Ошибка при парсинге {event_url}: {e}")
        return None


def get_coordinates_from_yandex(url):
    """Получаем координаты (lat, lon) из редиректа Яндекс.Карт"""
    try:
        time.sleep(random.uniform(2, 5))
        response = yandex_session.get(url, allow_redirects=False, timeout=15)
        print(f"        [INFO] Ответ от Яндекс.Карт: {response.status_code}")

        if response.status_code in (301, 302):
            redirect_url = response.headers.get('Location')
            print(f"        [DEBUG] redirect_url: {redirect_url}")

            parsed_url = urlparse(redirect_url)
            query_params = parse_qs(parsed_url.query)

            if 'll' in query_params:
                ll_value = unquote(query_params['ll'][0])
                print(f"        [DEBUG] Параметр ll: {ll_value}")
                lon_str, lat_str = ll_value.split(',')
                print(f"        [OK] Координаты: lat={lat_str}, lon={lon_str}")
                return float(lat_str), float(lon_str)
            else:
                print(f"        [WARN] В redirect_url нет параметра 'll'")
                return None, None
        else:
            print(f"        [WARN] Неожиданный код ответа: {response.status_code}")
            return None, None

    except Exception as e:
        print(f"        [ERROR] Ошибка при получении координат: {e}")
        return None, None

def deep_parse_location(link_point, client):
    """
    Возвращает глубокие поля для локации:
    latitude, longitude
    """
    map_url = get_yandex_map_link(link_point, client)

    if map_url:
        lat, lon = get_coordinates_from_yandex(map_url)
    else:
        lat, lon = None, None

    return {
        "latitude": lat,
        "longitude": lon
    }

def insert_location(site_row, deep_data, conn):
    query = text("""
        INSERT INTO s95_location (
            name_point,
            full_name_point,
            latitude,
            longitude,
            link_point
        )
        VALUES (
            :name_point,
            :full_name_point,
            :latitude,
            :longitude,
            :link_point
        )
    """)

    conn.execute(query, {
        "name_point": site_row['name_point'],
        "full_name_point": site_row['full_name_point'],
        "latitude": str(deep_data['latitude']) if deep_data['latitude'] is not None else None,
        "longitude": str(deep_data['longitude']) if deep_data['longitude'] is not None else None,
        "link_point": site_row['link_point'],
    })

def update_light_fields(site_row, conn):
    query = text("""
        UPDATE s95_location
        SET
            name_point = :name_point,
            full_name_point = :full_name_point
        WHERE link_point = :link_point
    """)

    conn.execute(query, {
        "name_point": site_row['name_point'],
        "full_name_point": site_row['full_name_point'],
        "link_point": site_row['link_point'],
    })

def update_location_with_deep_fill(site_row, db_row, deep_data, conn):
    new_latitude = db_row['latitude']
    new_longitude = db_row['longitude']

    if (pd.isna(new_latitude) or new_latitude is None or str(new_latitude).strip() == "") and deep_data['latitude'] is not None:
        new_latitude = str(deep_data['latitude'])

    if (pd.isna(new_longitude) or new_longitude is None or str(new_longitude).strip() == "") and deep_data['longitude'] is not None:
        new_longitude = str(deep_data['longitude'])

    query = text("""
        UPDATE s95_location
        SET
            name_point = :name_point,
            full_name_point = :full_name_point,
            latitude = :latitude,
            longitude = :longitude
        WHERE link_point = :link_point
    """)

    conn.execute(query, {
        "name_point": site_row['name_point'],
        "full_name_point": site_row['full_name_point'],
        "latitude": new_latitude,
        "longitude": new_longitude,
        "link_point": site_row['link_point'],
    })

def count_locations_missing_deep(engine):
    query = """
        SELECT COUNT(*) AS cnt
        FROM s95_location
        WHERE link_point IS NOT NULL
          AND (
              latitude IS NULL OR trim(latitude) = ''
              OR longitude IS NULL OR trim(longitude) = ''
          )
    """
    df = pd.read_sql(query, engine)
    return int(df.iloc[0]['cnt'])

def get_locations_missing_deep(engine, limit=0):
    query = """
        SELECT
            name_point,
            full_name_point,
            latitude,
            longitude,
            link_point
        FROM s95_location
        WHERE link_point IS NOT NULL and is_pause is not true
          AND (
              latitude IS NULL OR trim(latitude) = ''
              OR longitude IS NULL OR trim(longitude) = ''
          )
        ORDER BY name_point
    """

    if limit and limit > 0:
        query += f" LIMIT {limit}"

    return pd.read_sql(query, engine)
# --------------------- Основной запуск ---------------------

if __name__ == "__main__":
    client = S95HttpClient(
        connect_timeout=10,
        read_timeout=30,
        min_delay=18.0,
        max_delay=40.0,
        cooldown_seconds=1800,
        max_retries=2,
    )

    engine = create_engine(credential)

    MIN_SLEEP_BETWEEN_POINTS = 120
    MAX_SLEEP_BETWEEN_POINTS = 240

    SESSION_RESET_MIN = 3
    SESSION_RESET_MAX = 6
    processed_points = 0
    next_reset_at = random.randint(SESSION_RESET_MIN, SESSION_RESET_MAX)

    mode = input(
        "Выберите режим:\n"
        "1 — лёгкий проход (списки локаций, синхронизация БД, статистика)\n"
        "2 — глубокий проход (страницы парков, координаты)\n"
        "Ваш выбор: "
    ).strip()

    if mode == "1":
        site_df = collect_site_locations(client)
        db_df = load_existing_locations(engine)

        db_by_link = {}
        if not db_df.empty:
            for _, row in db_df.iterrows():
                db_by_link[row['link_point']] = row

        stats = {
            "inserted_new": 0,
            "updated_light": 0,
            "deep_missing_existing": 0,
            "unchanged": 0
        }

        for idx, site_row in site_df.iterrows():
            link_point = site_row['link_point']
            db_row = db_by_link.get(link_point)

            action = classify_location(site_row, db_row)
            print(f"\n[STEP] {site_row['name_point']} | action={action}")

            try:
                with engine.begin() as conn:
                    if action == "insert_new":
                        insert_location(
                            site_row,
                            {"latitude": None, "longitude": None},
                            conn
                        )
                        stats["inserted_new"] += 1

                    elif action == "update_light_only":
                        update_light_fields(site_row, conn)
                        stats["updated_light"] += 1

                    elif action == "deep_fill_missing":
                        # В режиме 1 глубоко не парсим, только считаем такие парки
                        stats["deep_missing_existing"] += 1

                    elif action == "skip":
                        stats["unchanged"] += 1

            except Exception as e:
                print(f"[ERROR] Ошибка для {link_point}: {e}")
                continue

        missing_deep_total = count_locations_missing_deep(engine)

        print("\n[RESULT] Статистика лёгкого прохода:")
        print(f"  Добавлено новых парков: {stats['inserted_new']}")
        print(f"  Обновлено лёгких полей: {stats['updated_light']}")
        print(f"  Уже существовали, но без deep-данных: {stats['deep_missing_existing']}")
        print(f"  Без изменений: {stats['unchanged']}")
        print(f"  Всего парков без координат в БД после прохода: {missing_deep_total}")

    elif mode == "2":
        raw_limit = input("Сколько парков обработать глубоко? 0 = все: ").strip()
        limit = int(raw_limit) if raw_limit else 0

        missing_df = get_locations_missing_deep(engine, limit=limit)

        print(f"\n[INFO] Найдено парков без координат: {len(missing_df)}")

        stats = {
            "filled_deep": 0,
            "failed": 0
        }

        for idx, db_row in missing_df.iterrows():
            link_point = db_row['link_point']
            name_point = db_row['name_point']

            print(f"\n[STEP] Deep parse: {name_point} | {link_point}")

            try:
                deep_data = deep_parse_location(link_point, client)

                with engine.begin() as conn:
                    update_location_with_deep_fill(db_row, db_row, deep_data, conn)

                stats["filled_deep"] += 1

                processed_points += 1

                if processed_points >= next_reset_at:
                    print(f"        [INFO] Reset session после {processed_points} локаций")
                    client.reset_session()
                    time.sleep(random.uniform(10, 30))
                    processed_points = 0
                    next_reset_at = random.randint(SESSION_RESET_MIN, SESSION_RESET_MAX)

                sleep_time = random.uniform(MIN_SLEEP_BETWEEN_POINTS, MAX_SLEEP_BETWEEN_POINTS)
                print(f"        [INFO] Ждём {sleep_time:.0f} секунд перед следующей локацией")
                time.sleep(sleep_time)

            except Exception as e:
                print(f"[ERROR] Ошибка deep parse для {link_point}: {e}")
                stats["failed"] += 1
                continue

        print("\n[RESULT] Статистика глубокого прохода:")
        print(f"  Успешно дозаполнено парков: {stats['filled_deep']}")
        print(f"  Ошибок: {stats['failed']}")
        print(f"  Осталось парков без координат: {count_locations_missing_deep(engine)}")

    else:
        print("Неизвестный режим. Нужно выбрать 1 или 2.")
