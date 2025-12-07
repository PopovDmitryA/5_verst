import json
from datetime import datetime
from pathlib import Path

import configparser
import pandas as pd
import requests
import sqlalchemy as sa
from typing import Optional
from bs4 import BeautifulSoup
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from db import get_january_subscribed_tg_ids


date_start = datetime.now()
print(date_start, 'Начало работы скрипта')

# --- Пути и конфиг ---
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
CONFIG_PATH = PROJECT_ROOT / "5_verst.ini"

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

db_host = config['five_verst_stats']['host']
db_user = config['five_verst_stats']['username']
db_pass = config['five_verst_stats']['password']
db_name = config['five_verst_stats']['dbname']

tg_token = config['telegram']['token']

# админ(ы) для отчёта — список chat_id через запятую
admin_chat_ids_raw = config['telegram'].get('admins', '').strip()
if admin_chat_ids_raw:
    admin_chat_ids = [x.strip() for x in admin_chat_ids_raw.split(',') if x.strip()]
else:
    admin_chat_ids = []

credential = f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}'
engine = sa.create_engine(credential)


# --- Служебные функции ---


def add_update_table(engine_, table_name: str, upd_time: datetime):
    """Логируем время обновления таблицы."""
    Session = sessionmaker(bind=engine_)
    session = Session()
    insert_query = text("""
        INSERT INTO update_table (table_name, update_date)
        VALUES (:table_name, :update_date);
    """)
    session.execute(insert_query, {"table_name": table_name, "update_date": upd_time})
    session.commit()
    session.close()


def send_telegram_message(token: str, chat_id: str, text_msg: str):
    """Отправка сообщения в Telegram."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": text_msg,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
        )
        if not resp.ok:
            print(f"Не удалось отправить сообщение в Telegram ({chat_id}):", resp.text)
        else:
            print(f"Сообщение в Telegram отправлено ({chat_id}).")
    except Exception as e:
        print("Ошибка при отправке сообщения в Telegram:", e)


def ensure_january2026_schema(engine_):
    """Гарантируем, что в january2026 есть колонка city."""
    with engine_.begin() as conn:
        conn.execute(text("ALTER TABLE january2026 ADD COLUMN IF NOT EXISTS city text;"))


def fetch_additional_events() -> pd.DataFrame:
    """
    Тянем с сайта данные дополнительных стартов и возвращаем DataFrame
    с колонками: name_point, latitude, longitude, time_start, city
    (только для тех локаций, у которых заявлен старт на странице).
    """
    site = "https://5verst.ru/additional-events/"

    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) "
        "Gecko/20100101 Firefox/133.0"
    )

    resp = requests.get(site, headers={"User-Agent": user_agent}, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.content, "lxml")

    # ищем любой тег с атрибутом data-geojson
    map_div = soup.find(attrs={"data-geojson": True})
    if map_div is None:
        raise RuntimeError("Не найден блок с атрибутом data-geojson.")

    geojson_raw = map_div.get("data-geojson")
    if not geojson_raw:
        raise RuntimeError("Атрибут data-geojson пуст.")

    geo = json.loads(geojson_raw)

    def feature_to_row(feature: dict):
        """Из одного feature достаём строку."""
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})
        coords = geom.get("coordinates", [None, None])

        add_time = props.get("additionalStartTime") or {}
        hour = add_time.get("hour")
        minute = add_time.get("minute")
        if hour is None or minute is None:
            return None

        name_point = props.get("title") or props.get("iconCaption")
        if not name_point:
            return None

        longitude = coords[0]
        latitude = coords[1]
        time_start = f"{int(hour):02d}:{int(minute):02d}"

        return {
            "name_point": name_point,
            "latitude": latitude,
            "longitude": longitude,
            "time_start": time_start,
        }

    rows = []

    # Основной вариант структуры: result.list[*].feature_collection.features[*]
    if isinstance(geo, dict) and isinstance(geo.get("result"), dict) and "list" in geo["result"]:
        for item in geo["result"]["list"]:
            fc = item.get("feature_collection") or {}
            for f in fc.get("features", []):
                row = feature_to_row(f)
                if row:
                    rows.append(row)
    # Запасные варианты
    elif isinstance(geo, dict) and "features" in geo:
        for f in geo["features"]:
            row = feature_to_row(f)
            if row:
                rows.append(row)
    elif isinstance(geo, list):
        for f in geo:
            row = feature_to_row(f)
            if row:
                rows.append(row)
    else:
        raise RuntimeError("Неизвестный формат geojson для дополнительных стартов.")

    df = pd.DataFrame(rows, columns=["name_point", "latitude", "longitude", "time_start"])

    print("Собрал данные с сайта, строк (только локации со стартами):", len(df))

    # Подтягиваем города
    loc_df = pd.read_sql("SELECT name_point, city FROM general_location", con=engine)
    df = df.merge(loc_df, on="name_point", how="left")

    # city может быть NaN -> заменим на None
    df["city"] = df["city"].where(df["city"].notna(), None)

    return df


# --- Основная логика ---


# 0. Гарантируем, что в january2026 есть колонка city
ensure_january2026_schema(engine)

# 1. Тянем актуальные данные с сайта (ТОЛЬКО локации со стартами)
site_starts = fetch_additional_events()
print("Первые строки новых данных (с сайта):")
print(site_starts.head())

# 1.1. Формируем полный список локаций из general_location
#      и проставляем time_start: либо из сайта, либо 'no_info', если старта нет.
loc_full = pd.read_sql(
    "SELECT name_point, latitude, longitude, city FROM general_location",
    con=engine
)

# Берём только name_point + time_start из site_starts
site_times = site_starts[["name_point", "time_start"]]

# Левый join: все локации из general_location, где есть старт — подтягиваем время
january_new = loc_full.merge(site_times, on="name_point", how="left")

# Локации без старта на сайте помечаем 'no_info'
january_new["time_start"] = january_new["time_start"].fillna("no_info")

print("Первые строки полного списка (все локации):")
print(january_new.head())

# 2. Читаем текущие данные из january2026
try:
    old_df = pd.read_sql(
        "SELECT name_point, latitude, longitude, time_start, city FROM january2026",
        con=engine
    )
except Exception as e:
    print("Не удалось прочитать january2026, считаем, что таблица пустая:", e)
    old_df = pd.DataFrame(columns=["name_point", "latitude", "longitude", "time_start", "city"])

# Страхуемся, чтобы нужные колонки точно были
for col in ["name_point", "latitude", "longitude", "time_start", "city"]:
    if col not in old_df.columns:
        old_df[col] = None

# Приводим типы
old_df["name_point"] = old_df["name_point"].astype(str)
january_new["name_point"] = january_new["name_point"].astype(str)

# 3. Сравниваем старое и новое по name_point
old_df = old_df.set_index("name_point")
new_df = january_new.set_index("name_point")

all_points = sorted(set(old_df.index) | set(new_df.index))

changes = []


def normalize_time(value: Optional[str]) -> str:
    """
    Нормализуем время:
    - None и 'no_info' считаем одним состоянием: 'no_info'
    - всё остальное оставляем как есть (например, '09:00').
    """
    if value is None or value == "no_info":
        return "no_info"
    return value


def display_old_new(old_norm: str, new_norm: str) -> tuple[str, str]:
    """
    Возвращаем человекочитаемые "Было"/"Стало" по нормализованным значениям.
    Логика:
      - 'no_info' → "старт не заявлен"
      - если было время, а стало 'no_info' → "старт отменён"
    """
    # Было
    if old_norm == "no_info":
        old_display = "старт не заявлен"
    else:
        old_display = old_norm

    # Стало
    if new_norm == "no_info":
        if old_norm == "no_info":
            new_display = "старт не заявлен"
        else:
            new_display = "старт отменён"
    else:
        new_display = new_norm

    return old_display, new_display


for point in all_points:
    old_row = old_df.loc[point] if point in old_df.index else None
    new_row = new_df.loc[point] if point in new_df.index else None

    old_time_raw = old_row["time_start"] if old_row is not None else None
    new_time_raw = new_row["time_start"] if new_row is not None else None

    old_norm = normalize_time(old_time_raw)
    new_norm = normalize_time(new_time_raw)

    # Если нормализованные значения одинаковы — изменений нет
    if old_norm == new_norm:
        continue

    old_city = old_row["city"] if (old_row is not None and "city" in old_row) else None
    new_city = new_row["city"] if (new_row is not None and "city" in new_row) else None
    city_display = new_city or old_city or "город не указан"

    old_display, new_display = display_old_new(old_norm, new_norm)

    changes.append(
        {
            "name_point": point,
            "city": city_display,
            "old_time_display": old_display,
            "new_time_display": new_display,
            "old_norm": old_norm,
            "new_norm": new_norm,
        }
    )

print(f"Найдено изменений: {len(changes)}")

# 4. Если есть изменения — формируем и отправляем сообщение в Telegram
sent_count = 0  # сколько сообщений реально попытались разослать

if changes:
    lines = [
        "🤖 Автоматическое уведомление",
        "",
        "Обновления по стартам 1 января:",
    ]

    for ch in changes:
        lines.append("")  # пустая строка между блоками
        # Локация жирным + эмодзи
        lines.append(f"📍 Локация: <b>{ch['name_point']}</b> ({ch['city']})")
        lines.append(f"Было: {ch['old_time_display']}")
        lines.append(f"Стало: {ch['new_time_display']}")

    # ссылка на карту в конце сообщения
    lines.append("")
    lines.append("Посмотреть данные на карте: https://5verst.ru/additional-events/")

    msg_text = "\n".join(lines)
    print("Есть изменения, отправляю сообщение в Telegram...")
    print(msg_text)  # на всякий случай выводим в консоль

    targets = get_january_subscribed_tg_ids()

    for tg_id in targets:
        send_telegram_message(tg_token, tg_id, msg_text)
        sent_count += 1
else:
    print("Изменений по стартам нет, сообщение в Telegram не отправляем.")

# 4.1. Отправляем отчёт админу, если есть изменения и есть кому слать
if changes and admin_chat_ids:
    summary_lines = [
        "📊 Отчёт по рассылке уведомлений 1 января",
        "",
        f"Изменений в стартах: {len(changes)}",
        f"Подписчиков (по БД): {len(get_january_subscribed_tg_ids())}",
        f"Сообщений отправлено (попыток): {sent_count}",
    ]
    summary_text = "\n".join(summary_lines)

    for admin_id in admin_chat_ids:
        send_telegram_message(tg_token, admin_id, summary_text)

# 5. Обновляем january2026 в БД
with engine.begin() as conn:
    conn.execute(text("TRUNCATE january2026;"))
    january_new.to_sql("january2026", con=conn, if_exists="append", index=False)

current_datetime = datetime.now()
add_update_table(engine, "january2026", current_datetime)

print("Завершена работа скрипта")
