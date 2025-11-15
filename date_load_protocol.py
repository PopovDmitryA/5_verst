from datetime import datetime
from datetime import date
import sqlalchemy as sa
import pandas as pd
import configparser
import requests
from pathlib import Path
import re
from bs4 import BeautifulSoup
import time
from io import StringIO

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "5_verst.ini"

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

db_host = config['five_verst_stats']['host']
db_user = config['five_verst_stats']['username']
db_pass = config['five_verst_stats']['password']
db_name = config['five_verst_stats']['dbname']

credential = f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}'

RU_MONTH_MAP = {
    # Январь
    "январь": "January", "января": "January", "январе": "January",
    # Февраль
    "февраль": "February", "февраля": "February", "феврале": "February",
    # Март
    "март": "March", "марта": "March", "марте": "March",
    # Апрель
    "апрель": "April", "апреля": "April", "апреле": "April",
    # Май
    "май": "May", "мая": "May", "мае": "May",
    # Июнь
    "июнь": "June", "июня": "June", "июне": "June",
    # Июль
    "июль": "July", "июля": "July", "июле": "July",
    # Август
    "август": "August", "августа": "August", "августе": "August",
    # Сентябрь
    "сентябрь": "September", "сентября": "September", "сентябре": "September",
    # Октябрь
    "октябрь": "October", "октября": "October", "октябре": "October",
    # Ноябрь
    "ноябрь": "November", "ноября": "November", "ноябре": "November",
    # Декабрь
    "декабрь": "December", "декабря": "December", "декабре": "December",
}
MONTH_ORDER = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

def _fetch_html(url: str, tries: int = 3, pause: float = 2.0) -> str:
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/127.0.0 Safari/537.36"),
        "Accept-Language": "ru,en;q=0.9",
        "Referer": "https://5verst.ru/",
    }
    for i in range(tries):
        try:
            r = requests.get(url, headers=headers, timeout=20)
            if r.status_code == 200:
                if not r.encoding or r.encoding.lower() == "iso-8859-1":
                    r.encoding = r.apparent_encoding
                return r.text
            if r.status_code in (403, 429):
                time.sleep(pause * (i + 1))
                continue
            r.raise_for_status()
        except Exception:
            time.sleep(pause * (i + 1))

    try:
        import cloudscraper
        scraper = cloudscraper.create_scraper()
        r = scraper.get(url, timeout=30)
        if r.status_code == 200:
            return r.text
        raise RuntimeError(f"cloudscraper status={r.status_code}")
    except Exception as e:
        raise RuntimeError(f"Не удалось получить HTML: {e}")

def update_date_load():
    current_datetime = datetime.now()
    print(f'Запуск функции update_date_load() в {current_datetime}')

    # 1️⃣ Скачиваем страницу с нужными заголовками (или через cloudscraper)
    site = 'https://5verst.ru/results/latest/'
    html = _fetch_html(site)  # <-- новая функция, как в других скриптах

    # 2️⃣ Парсим таблицу не напрямую по ссылке, а из HTML
    last_event = pd.read_html(StringIO(html), flavor="lxml")[0]

    # 3️⃣ Остальной код без изменений
    last_event['date_type_event'] = last_event['Дата'].copy()
    last_event['date_type_event'] = pd.to_datetime(last_event['date_type_event'], format='%d.%m.%Y')
    last_event['name_point'] = last_event['Старт #'].str.split(' #').str[0]

    engine = sa.create_engine(credential)
    result = pd.read_sql("SELECT * FROM general_date_load_protocol", con=engine)

    new_protocol = 0

    for _, row in last_event.iterrows():
        name_point = row['name_point']
        if '#' not in row['Старт #']:
            continue
        date_event = row['date_type_event']
        exists = ((result['name_point'] == name_point) &
                  (result['date_event'] == date_event)).any()
        if not exists:
            with engine.begin() as connection:
                query = sa.text(
                    "SELECT link_point FROM general_link_all_location WHERE name_point = :name_point"
                )
                link_point = connection.execute(query, {"name_point": name_point}).scalar()
                if not link_point:
                    print(f"[WARN] Для парка '{name_point}' нет ссылки — пропускаю")
                    continue
                try:
                    time_start = get_start_time(extract_schedule(link_point))
                except Exception as e:
                    print(f"[ERROR] Не удалось получить время старта для '{name_point}': {e}")
                    time_start = None
                insert_query = sa.text("""
                    INSERT INTO general_date_load_protocol (name_point, date_event, date_load, start_time)
                    VALUES (:name_point, :date_event, :date_load, :start_time)
                    ON CONFLICT (name_point, date_event) DO UPDATE 
                    SET name_point = EXCLUDED.name_point, 
                        date_event = EXCLUDED.date_event;
                """)
                connection.execute(insert_query, {
                    'name_point': name_point,
                    'date_event': date_event,
                    'date_load': current_datetime,
                    'start_time': time_start
                })
            new_protocol += 1
            print(f'Для парка {name_point} протокол от {date_event} записан в {current_datetime}')


def _norm_month(token: str) -> str:
    w = token.strip(" .,:;!?()\"'").lower().replace("ё", "е")
    if w in RU_MONTH_MAP:
        return RU_MONTH_MAP[w]
    # fallback: проверяем по стему
    for ru, en in RU_MONTH_MAP.items():
        if w.startswith(ru[:4]):
            return en
    raise ValueError(f"Не распознал месяц: {token!r}")


def _fmt_time(hhmm: str) -> str:
    hhmm = hhmm.replace(".", ":")
    h, m = hhmm.split(":")
    return f"{int(h):02d}:{int(m):02d}:00"


def extract_schedule(url: str):
    """
    Парсит страницу парка и извлекает расписание стартов.
    Теперь использует _fetch_html() вместо requests.get()
    — с заголовками и защитой от 403.
    """
    html = _fetch_html(url)  # ✅ защищённая загрузка HTML
    soup = BeautifulSoup(html, 'lxml')

    blocks = soup.find_all('div', {'class': 'knd-block-info__col'})
    if len(blocks) < 2:
        return []

    paragraph = blocks[1].find('p')
    if not paragraph:
        return []

    raw = paragraph.get_text(" ", strip=True)
    # Нормализация пробелов и тире
    raw = raw.replace("\xa0", " ")
    raw = re.sub(r"[-–—]", "-", raw)
    raw = re.sub(r"\s+", " ", raw)
    seg_low = raw.lower()

    results = []

    # Ищем блоки с временем
    time_block_re = re.compile(r'(?:в\s*)?(?P<time>\d{1,2}[:.]\d{2})\s*\((?P<inside>[^)]+)\)')
    for m in time_block_re.finditer(seg_low):
        time_str = _fmt_time(m.group("time"))
        inside = m.group("inside").strip()

        # убираем "в " в начале
        inside = re.sub(r'^в\s+', '', inside)

        # с ... по ...
        m_s_po = re.match(r'с\s+([а-яё]+)\s+по\s+([а-яё]+)', inside)
        if m_s_po:
            results.append({
                "period_start": _norm_month(m_s_po.group(1)),
                "period_finish": _norm_month(m_s_po.group(2)),
                "time_start": time_str
            })
            continue

        # месяц-месяц
        m_range = re.match(r'([а-яё]+)\s*-\s*([а-яё]+)', inside)
        if m_range:
            results.append({
                "period_start": _norm_month(m_range.group(1)),
                "period_finish": _norm_month(m_range.group(2)),
                "time_start": time_str
            })
            continue

        # перечисление месяцев
        tokens = re.split(r",|\s+и\s+", inside)
        months = [_norm_month(tok) for tok in tokens if tok.strip()]
        if months:
            order = [MONTH_ORDER.index(mm) for mm in months]
            start = months[order.index(min(order))]
            finish = months[order.index(max(order))]
            results.append({
                "period_start": start,
                "period_finish": finish,
                "time_start": time_str
            })

    # fallback — если ничего не найдено
    if not results:
        time_only = re.search(r'(\d{1,2}[:.]\d{2})', seg_low)
        if time_only:
            results.append({
                "period_start": "January",
                "period_finish": "December",
                "time_start": _fmt_time(time_only.group(1))
            })

    return results



def get_start_time(schedule: list[dict], on_date: date = None) -> str:
    """
    schedule = [
        {'period_start': 'September', 'period_finish': 'May', 'time_start': '09:00:00'},
        {'period_start': 'June', 'period_finish': 'August', 'time_start': '08:00:00'}
    ]
    """
    if on_date is None:
        on_date = date.today()

    month_name = on_date.strftime("%B")  # English full month name, e.g. "August"
    month_idx = MONTH_ORDER.index(month_name)

    for entry in schedule:
        start_idx = MONTH_ORDER.index(entry["period_start"])
        finish_idx = MONTH_ORDER.index(entry["period_finish"])

        if start_idx <= finish_idx:
            # обычный случай (например March–October)
            if start_idx <= month_idx <= finish_idx:
                t = datetime.strptime(entry["time_start"], "%H:%M:%S").time()
                return t
        else:
            # "перелом через декабрь" (например September–May)
            if month_idx >= start_idx or month_idx <= finish_idx:
                t = datetime.strptime(entry["time_start"], "%H:%M:%S").time()
                return t

    return None  # если ничего не подошло


try:
    update_date_load()
    #print(extract_schedule('https://5verst.ru/pyatigorskkomsomolsky/'))
    # engine = sa.create_engine(credential)
    # all_link = pd.read_sql(f"select * from general_link_all_location", con=engine)
    # print(all_link)
    # for index, row in all_link.iterrows():
    #     print(f'В парке {row["name_point"]} старт в {get_start_time(extract_schedule(row["link_point"]))}')
except Exception as e:
    print(f'Ошибка {e}')
finally:
    # print('Завершение работы скрипта')
    print('--------------------------------')