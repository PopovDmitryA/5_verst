import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, unquote
import time
import random
from sqlalchemy import create_engine
from pathlib import Path
import configparser

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

def list_location(domen):
    """Парсим список локаций"""
    print(f"\n[INFO] Парсим список локаций для домена: {domen}")
    page = f"https://s95.{domen}/events"
    t = requests.get(page)
    soup = BeautifulSoup(t.text, "html.parser")

    table_last_event = soup.find('div', {'class': 'row row-cols-1 row-cols-md-2 g-3'})
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


def get_yandex_map_link(event_url):
    """Получает ссылку на Яндекс.Карты из блока 'Наши контакты'"""
    print(f"    [INFO] Парсим страницу события: {event_url}")
    try:
        t = requests.get(event_url, timeout=10)
        soup = BeautifulSoup(t.text, 'html.parser')

        contact_titles = ['Наши контакты', 'Контакти', 'Нашы кантакты']

        contacts_block = None
        for title in contact_titles:
            contacts_block = soup.find('section', string=title)
            if contacts_block:
                break

        if not contacts_block:
            print(f"    [WARN] Блок 'Наши контакты' не найден")
            return None

        card_body = contacts_block.find_parent('div', class_='card-header') \
                                  .find_next_sibling('div', class_='card-body')

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
        response = requests.get(url, allow_redirects=False, timeout=15)
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


def build_locations_df():
    """Собирает все локации из RU, BY, RS, находит карту и координаты"""
    df_all = pd.concat([
        list_location('ru'),
        list_location('by'),
        list_location('rs')
    ], ignore_index=True)

    print(f"\n[INFO] Всего найдено {len(df_all)} локаций по всем странам")

    map_links = []
    latitudes = []
    longitudes = []

    for idx, row in df_all.iterrows():
        print(f"\n[STEP] Обрабатываем: {row['full_name_point']}")
        map_url = get_yandex_map_link(row['link_point'])
        map_links.append(map_url)

        if map_url:
            lat, lon = get_coordinates_from_yandex(map_url)
        else:
            lat, lon = None, None

        latitudes.append(lat)
        longitudes.append(lon)

        # Рандомная задержка между запросами
        sleep_time = random.randint(1, 4)
        print(f"        [INFO] Ждём {sleep_time} секунд перед следующим запросом")
        time.sleep(sleep_time)

    df_all['map_url'] = map_links
    df_all['latitude'] = latitudes
    df_all['longitude'] = longitudes

    print("\n[INFO] Обработка завершена")
    return df_all


# --------------------- Запись в PostgreSQL ---------------------

def save_to_postgresql_append(df, table_name, db_url):
    """
    Сохраняет DataFrame в PostgreSQL с режимом append
    :param df: DataFrame для записи
    :param table_name: имя таблицы
    :param db_url: строка подключения SQLAlchemy
    """
    try:
        engine = create_engine(db_url)
        df_to_save = df[['name_point', 'full_name_point', 'latitude', 'longitude', 'link_point']]
        df_to_save.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"\n[INFO] Данные успешно добавлены в таблицу {table_name}")
    except Exception as e:
        print(f"\n[ERROR] Ошибка при записи в БД: {e}")


# --------------------- Основной запуск ---------------------

if __name__ == "__main__":
    df_locations = build_locations_df()
    save_to_postgresql_append(df_locations, "s95_location", credential)

    print("\n[RESULT] Итоговый датафрейм:")
    print(df_locations)
