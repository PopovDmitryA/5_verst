import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from io import StringIO

UA_HDRS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/127.0.0 Safari/537.36"),
    "Accept-Language": "ru,en;q=0.9",
    "Referer": "https://5verst.ru/",
}

def _fetch_html(url: str, tries: int = 3, pause: float = 2.0) -> str:
    # 1) обычный requests с нормальными заголовками
    for i in range(tries):
        try:
            r = requests.get(url, headers=UA_HDRS, timeout=20, allow_redirects=True)
            if r.status_code == 200:
                if not r.encoding or r.encoding.lower() == "iso-8859-1":
                    r.encoding = r.apparent_encoding
                return r.text
            # 403/429 — подождать и попробовать снова
            if r.status_code in (403, 429):
                time.sleep(pause * (i + 1))
                continue
            r.raise_for_status()
        except Exception:
            time.sleep(pause * (i + 1))

    # 2) fallback: cloudscraper (если стоит Cloudflare/WAF)
    try:
        import cloudscraper
        scraper = cloudscraper.create_scraper()
        r = scraper.get(url, timeout=30)
        if r.status_code == 200:
            return r.text
        raise RuntimeError(f"cloudscraper status={r.status_code}")
    except Exception as e:
        raise RuntimeError(f"Не удалось получить HTML: {e}")

def list_protocols_in_park(link):
    """Парсим страницу с последними пробежками по парку"""
    # Получаем HTML с нужными заголовками (или через cloudscraper, если нужно)
    html = _fetch_html(link)
    soup = BeautifulSoup(html, "lxml")

    # Извлекаем название локации
    name_point_parse = soup.find_all(
        "li", class_="menu-item menu-item-type-custom menu-item-object-custom"
    )
    name_point = name_point_parse[2].get_text().strip() if len(name_point_parse) > 2 else None

    # Ищем таблицу с протоколами
    table_protocols = soup.find("table", {
        "class": "sortable results-table min-w-full leading-normal"
    })
    if not table_protocols:
        raise RuntimeError("Не найдена таблица с протоколами на странице")

    rows = table_protocols.find_all("tr")
    data = []
    for row in rows:
        cols = [col.text.strip() for col in row.find_all(["td", "th"])]
        find_link = row.find("a")
        if find_link is not None:
            name_runner, link_runner = find_link.text.strip(), find_link.get("href")
            cols += [name_runner, link_runner]
        data.append(cols)

    # Добавляем названия колонок
    if not data:
        raise RuntimeError("Таблица пуста или не распознана")

    data[0] += ["date_event", "link_event"]
    df = pd.DataFrame(data[1:], columns=data[0])
    df["name_point"] = name_point

    return df

def transform_df_list_protocol(list_protocol):
    '''Преобразуем таблицу в формат для БД'''
    df_copy = list_protocol.copy()
    df_copy = df_copy.drop(columns=['Дата'])

    # Переименовываем столбцы
    df_copy = df_copy.rename(columns={"##": "index_event",
                                      "Финишёров": "count_runners",
                                      "Волонтёров": "count_vol",
                                      "Среднее время": "mean_time",
                                      'Лучшее "Ж"': "best_time_woman",
                                      'Лучшее "М"': "best_time_man"})

    df_copy['is_test'] = df_copy['index_event'].apply(lambda x: pd.isnull(x) or x == '')
    # Меняем порядок столбцов
    df_copy = df_copy[["index_event",
                       "name_point",
                       "date_event",
                       "link_event",
                       "is_test",
                       "count_runners",
                       'count_vol',
                       'mean_time',
                       'best_time_woman',
                       'best_time_man']]  # Новый порядок
    df_copy['index_event'] = df_copy['index_event'].replace('', 0).astype(int)
    df_copy['date_event'] = pd.to_datetime(df_copy['date_event'], format='%d.%m.%Y', errors='coerce')
    for col in ['mean_time', 'best_time_woman', 'best_time_man']:
        df_copy[col] = df_copy[col].replace('', pd.NA).fillna('00:00:00')
        df_copy[col] = pd.to_datetime(df_copy[col], format='%H:%M:%S', errors='coerce').dt.time

    for col in ['index_event', 'count_runners', 'count_vol']:
        df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').astype('Int64')

    return df_copy