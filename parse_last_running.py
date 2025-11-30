import pandas as pd
import requests
import link_handler
from bs4 import BeautifulSoup


def last_event_parse():
    """Парсим страницу с последними пробежками всех парков."""
    url = "https://5verst.ru/results/latest/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/127.0.0.1 Safari/537.36",
        "Accept-Language": "ru,en;q=0.9",
        "Referer": "https://5verst.ru/",
    }

    try:
        # Можно чуть увеличить таймаут, но без фанатизма
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
    except requests.exceptions.Timeout:
        print(f"⏱️ Таймаут при обращении к {url}. "
              f"Сайт не ответил за отведённое время.")
        return None
    except requests.exceptions.RequestException as e:
        # Сюда попадают все сетевые ошибки: ConnectionError, HTTPError и т.п.
        print(f"❌ Ошибка при обращении к {url}: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    table_last_event = soup.find(
        "table",
        {"class": "sortable results-table min-w-full leading-normal"}
    )
    if table_last_event is None:
        print("❌ Не удалось найти таблицу последних забегов на странице. "
              "Возможно, изменился HTML 5verst.ru.")
        return None

    rows = table_last_event.find_all("tr")
    data = []

    for index, row in enumerate(rows):
        cols = [col.text.strip() for col in row.find_all(["td", "th"])]
        find_link = row.find("a")
        if find_link is not None:
            link_runner = find_link.get("href")
            cols += [link_runner]
        data.append(cols)

    if not data:
        print("⚠️ Таблица последних забегов пуста или не распарсилась.")
        return None

    # Добавляем заголовок для ссылки на событие
    data[0] += ["link_event"]
    df = pd.DataFrame(data[1:], columns=data[0])

    return df


def transform_df_last_event(df):
    """Преобразуем таблицу в формат для БД.

    Если df = None или пустой → возвращаем пустой датафрейм
    с нужными колонками, чтобы дальше код не падал.
    """
    expected_columns = [
        "index_event",
        "name_point",
        "date_event",
        "link_event",
        "link_point",
        "is_test",
        "count_runners",
        "count_vol",
        "mean_time",
        "best_time_woman",
        "best_time_man",
    ]

    if df is None or df.empty:
        print("ℹ️ Нет данных по последним забегам (df пустой). "
              "Пропускаем обновление по последним протоколам.")
        return pd.DataFrame(columns=expected_columns)

    df_copy = df.copy()
    df_copy['name_point'] = df_copy['Старт #'].apply(lambda x: x.split('#')[0].strip())
    df_copy['index_event'] = df_copy['Старт #'].apply(
        lambda x: x.split('#', 1)[-1].strip() if '#' in x else '0'
    )
    df_copy['is_test'] = df_copy['Старт #'].apply(lambda x: False if '#' in x else True)

    df_copy['link_point'] = df_copy['link_event'].apply(link_handler.main_link_event)
    df_copy['date_event'] = pd.to_datetime(df_copy['Дата'], format='%d.%m.%Y')

    df_copy['link_event'] = df_copy.apply(
        lambda row: link_handler.link_protocol_from_date(row['link_point'], row['date_event']),
        axis=1
    )
    df_copy = df_copy.drop(columns=['Старт #', 'Дата'])

    df_copy = df_copy.rename(columns={
        "Финишёров": "count_runners",
        "Волонтёров": "count_vol",
        "Среднее время": "mean_time",
        'Лучшее "Ж"': "best_time_woman",
        'Лучшее "М"': "best_time_man",
    })

    df_copy = df_copy[[
        "index_event",
        "name_point",
        "date_event",
        "link_event",
        "link_point",
        "is_test",
        "count_runners",
        "count_vol",
        "mean_time",
        "best_time_woman",
        "best_time_man",
    ]]

    for col in ['mean_time', 'best_time_woman', 'best_time_man']:
        df_copy[col] = df_copy[col].replace('', pd.NA).fillna('00:00:00')
        df_copy[col] = pd.to_datetime(df_copy[col], format='%H:%M:%S', errors='coerce').dt.time

    for col in ['index_event', 'count_runners', 'count_vol']:
        df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').astype('Int64')

    return df_copy