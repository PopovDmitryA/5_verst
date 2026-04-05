import configparser
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Tuple

import pandas as pd
import requests
from geopy.distance import geodesic
from sqlalchemy import create_engine, text
from zoneinfo import ZoneInfo
from timezonefinder import TimezoneFinder


CREMLIN_COORDS = (55.7522, 37.6156)
_TF = TimezoneFinder(in_memory=True)


def add_distance_loc(latitude: float, longitude: float) -> Optional[float]:
    try:
        new_point = (float(latitude), float(longitude))
        return geodesic(CREMLIN_COORDS, new_point).kilometers
    except Exception:
        return None


def normalize_region(region: Optional[str]) -> Optional[str]:
    if not region:
        return region

    r = region.strip()

    suffixes = [
        " область",
        " обл.",
        " край",
        " республика",
        " респ.",
        " автономная область",
        " автономный округ",
        " ао",
        " АО",
        " округ",
        " г.",
        " город",
    ]

    for suffix in suffixes:
        if r.lower().endswith(suffix.lower()):
            r = r[:-len(suffix)].strip()
            break

    return r or None


def reverse_geocode_city_region(latitude: float, longitude: float) -> Tuple[Optional[str], Optional[str]]:
    url = "https://nominatim.openstreetmap.org/reverse"
    params = {
        "format": "jsonv2",
        "lat": latitude,
        "lon": longitude,
        "accept-language": "ru",
        "addressdetails": 1,
    }
    headers = {
        "User-Agent": "Popov_dmitry-bot/1.0 (contact: @Popov_dmitry)",
    }

    response = requests.get(url, params=params, headers=headers, timeout=20)
    if response.status_code != 200:
        return None, None

    data = response.json()
    address = data.get("address", {}) or {}

    city = (
        address.get("city")
        or address.get("town")
        or address.get("village")
        or address.get("hamlet")
        or address.get("municipality")
    )

    region = address.get("state") or address.get("region")
    region = normalize_region(region)

    return city, region


def tz_from_moscow(latitude: float, longitude: float) -> Optional[int]:
    try:
        tz_name = _TF.timezone_at(lat=latitude, lng=longitude)
        if not tz_name:
            return None

        now_utc = datetime.now(timezone.utc)
        moscow_offset = now_utc.astimezone(ZoneInfo("Europe/Moscow")).utcoffset()
        local_offset = now_utc.astimezone(ZoneInfo(tz_name)).utcoffset()

        if moscow_offset is None or local_offset is None:
            return None

        delta_hours = int((local_offset - moscow_offset).total_seconds() // 3600)
        return delta_hours
    except Exception:
        return None


def is_empty(value) -> bool:
    return pd.isna(value) or value is None


CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
CONFIG_PATH = PROJECT_ROOT / "5_verst.ini"

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

db_host = config["five_verst_stats"]["host"]
db_user = config["five_verst_stats"]["username"]
db_pass = config["five_verst_stats"]["password"]
db_name = config["five_verst_stats"]["dbname"]

credential = f"postgresql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(credential)

print("Подключение к базе выполнено.")

with engine.connect() as conn:
    df = pd.read_sql("""
        SELECT
            name_point,
            latitude,
            longitude,
            city,
            region,
            tz_from_moscow,
            distance_from_cremlin
        FROM s95_location
        WHERE city IS NULL
           OR region IS NULL
           OR tz_from_moscow IS NULL
           OR distance_from_cremlin IS NULL
    """, conn)

print(f"Считано {len(df)} строк, где пусто хотя бы одно из нужных полей.")

if df.empty:
    print("Нечего обновлять.")
    raise SystemExit

stats = {
    "total_rows": len(df),
    "without_coords": 0,
    "city_filled": 0,
    "region_filled": 0,
    "tz_filled": 0,
    "distance_filled": 0,
    "updated_rows": 0,
    "geocode_attempts": 0,
    "geocode_success": 0,
}

update_query = text("""
    UPDATE s95_location
    SET city = :city,
        region = :region,
        tz_from_moscow = :tz_from_moscow,
        distance_from_cremlin = :distance_from_cremlin
    WHERE name_point = :name_point
""")

with engine.begin() as conn:
    for _, row in df.iterrows():
        name_point = row["name_point"]
        latitude = row["latitude"]
        longitude = row["longitude"]

        if is_empty(latitude) or is_empty(longitude):
            stats["without_coords"] += 1
            print(f"[SKIP] {name_point} — нет координат, пропускаем.")
            continue

        latitude = float(latitude)
        longitude = float(longitude)

        new_city = row["city"]
        new_region = row["region"]
        new_tz = row["tz_from_moscow"]
        new_distance = row["distance_from_cremlin"]

        need_geocode = is_empty(row["city"]) or is_empty(row["region"])
        if need_geocode:
            stats["geocode_attempts"] += 1
            try:
                parsed_city, parsed_region = reverse_geocode_city_region(latitude, longitude)
                if parsed_city or parsed_region:
                    stats["geocode_success"] += 1

                if is_empty(row["city"]) and parsed_city:
                    new_city = parsed_city
                    stats["city_filled"] += 1

                if is_empty(row["region"]) and parsed_region:
                    new_region = parsed_region
                    stats["region_filled"] += 1

                time.sleep(1)
            except Exception as e:
                print(f"[WARN] {name_point} — ошибка reverse geocode: {e}")

        if is_empty(row["tz_from_moscow"]):
            try:
                parsed_tz = tz_from_moscow(latitude, longitude)
                if parsed_tz is not None:
                    new_tz = parsed_tz
                    stats["tz_filled"] += 1
            except Exception as e:
                print(f"[WARN] {name_point} — ошибка tz_from_moscow: {e}")

        if is_empty(row["distance_from_cremlin"]):
            try:
                parsed_distance = add_distance_loc(latitude, longitude)
                if parsed_distance is not None:
                    new_distance = parsed_distance
                    stats["distance_filled"] += 1
            except Exception as e:
                print(f"[WARN] {name_point} — ошибка расчёта distance_from_cremlin: {e}")

        changed = (
            new_city != row["city"]
            or new_region != row["region"]
            or new_tz != row["tz_from_moscow"]
            or new_distance != row["distance_from_cremlin"]
        )

        if not changed:
            print(f"[NO CHANGE] {name_point} — новых данных не найдено.")
            continue

        conn.execute(
            update_query,
            {
                "name_point": name_point,
                "city": new_city,
                "region": new_region,
                "tz_from_moscow": new_tz,
                "distance_from_cremlin": new_distance,
            }
        )

        stats["updated_rows"] += 1

        distance_str = f"{new_distance:.2f}" if new_distance is not None else "None"
        print(
            f"[UPDATED] {name_point} | "
            f"city={new_city}, region={new_region}, tz_from_moscow={new_tz}, distance_from_cremlin={distance_str}"
        )

print("\n=== ИТОГОВАЯ СТАТИСТИКА ===")
print(f"Всего строк выбрано:                  {stats['total_rows']}")
print(f"Пропущено без координат:             {stats['without_coords']}")
print(f"Попыток reverse geocode:             {stats['geocode_attempts']}")
print(f"Успешных reverse geocode:            {stats['geocode_success']}")
print(f"Заполнено city:                      {stats['city_filled']}")
print(f"Заполнено region:                    {stats['region_filled']}")
print(f"Заполнено tz_from_moscow:            {stats['tz_filled']}")
print(f"Заполнено distance_from_cremlin:     {stats['distance_filled']}")
print(f"Обновлено строк в БД:                {stats['updated_rows']}")
print("===========================\n")