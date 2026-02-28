import re
import configparser
from urllib.parse import urlparse, parse_qs
from typing import Optional, Tuple
from time import sleep
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from timezonefinder import TimezoneFinder

import requests
from bs4 import BeautifulSoup
from geopy.distance import geodesic
import sqlalchemy as sa

import link_handler
from parse_table_protocols_in_park import _fetch_html
from DB_handler import db_connect, info_table_update, update_view

def load_credential(ini_path: str = "5_verst.ini") -> str:
    config = configparser.ConfigParser()
    config.read(ini_path)

    db_host = config["five_verst_stats"]["host"]
    db_user = config["five_verst_stats"]["username"]
    db_pass = config["five_verst_stats"]["password"]
    db_name = config["five_verst_stats"]["dbname"]

    return f"postgresql://{db_user}:{db_pass}@{db_host}/{db_name}"

def extract_name_point(html: str) -> Optional[str]:
    """
    На главной странице 5verst обычно есть блок div.text со ссылкой,
    где текст вида '... 5 вёрст <НАЗВАНИЕ> ...'
    """
    soup = BeautifulSoup(html, "html.parser")
    text_block = soup.find("div", class_="text")
    if not text_block:
        return None

    for a in text_block.find_all("a"):
        park_name = a.get_text(strip=False)

        # берем часть строки после "5 вёрст"
        idx = park_name.find("5 вёрст")
        if idx == -1:
            continue

        start = idx + len("5 вёрст")
        # как в ноутбуке: ищем перевод строки после 3-го символа
        finish = park_name.find("\n", 3)
        if finish == -1:
            # fallback: до конца строки
            finish = len(park_name)

        name_point = park_name[start:finish].strip(" \t\r\n-–—")
        if name_point:
            return name_point

    return None


def try_extract_coords_from_meta(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    meta_tag = soup.find("meta", {"name": "geo.position"})
    if not meta_tag or not meta_tag.get("content"):
        return None, None

    content = meta_tag.get("content", "").strip()

    if ";" in content:
        parts = content.split(";")
    elif ":" in content:
        parts = content.split(":")
    else:
        return None, None

    if len(parts) < 2:
        return None, None

    return parts[0].strip(), parts[1].strip()


def try_extract_coords_from_meeting_point(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    meeting_point = soup.find("div", class_="meeting-point")
    if not meeting_point:
        meeting_point = soup.find("div", class_=lambda x: x and "meeting" in x.lower())

    if not meeting_point:
        return None, None

    a = meeting_point.find("a")
    if not a or not a.get_text(strip=True):
        return None, None

    coords_text = a.get_text(strip=True)
    if ":" in coords_text:
        parts = coords_text.split(":")
    elif ";" in coords_text:
        parts = coords_text.split(";")
    else:
        return None, None

    if len(parts) < 2:
        return None, None

    return parts[0].strip(), parts[1].strip()


def try_extract_coords_from_text(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    coord_pattern = r"(\d+\.\d+)[:;](\d+\.\d+)"
    main_content = soup.find("main") or soup.find("div", class_="entry-content") or soup.find("div", class_="page-content")
    text = main_content.get_text() if main_content else soup.get_text()
    m = re.search(coord_pattern, text)
    if not m:
        return None, None
    return m.group(1), m.group(2)


def try_extract_coords_from_yandex_links(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "yandex.ru/maps" in href or "yandex.com/maps" in href:
            # ищем pt=lon,lat
            parsed = urlparse(href)
            params = parse_qs(parsed.query)
            if "pt" in params and params["pt"]:
                pt_value = params["pt"][0]
                if "," in pt_value:
                    lon, lat = pt_value.split(",", 1)
                    return lat.strip(), lon.strip()
    return None, None


def clean_coord(x: Optional[str]) -> Optional[str]:
    if x is None:
        return None
    x = re.sub(r"[^\d\.\-]", "", str(x))
    return x if x else None

def normalize_region(region: Optional[str]) -> Optional[str]:
    if not region:
        return region

    r = region.strip()

    # убираем распространённые суффиксы (по краям строки)
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

    # удаляем только если это именно окончание
    for s in suffixes:
        if r.lower().endswith(s.lower()):
            r = r[: -len(s)].strip()
            break

    # частные случаи, если хочешь “Московская” вместо “Москва” и т.п. — сюда
    # (пока не трогаем)

    return r or None

def reverse_geocode_city_region(latitude: float, longitude: float) -> Tuple[Optional[str], Optional[str]]:
    """
    Возвращает (city, region) по координатам через Nominatim (OSM).
    ВАЖНО: сервис бесплатный, но есть ограничения по частоте запросов.
    """
    url = "https://nominatim.openstreetmap.org/reverse"
    params = {
        "format": "jsonv2",
        "lat": latitude,
        "lon": longitude,
        "accept-language": "ru",
        "addressdetails": 1,
    }

    # Nominatim требует нормальный User-Agent
    headers = {
        "User-Agent": "Popov_dmitry-bot/1.0 (contact: @Popov_dmitry)",
    }

    r = requests.get(url, params=params, headers=headers, timeout=20)
    if r.status_code != 200:
        return None, None

    data = r.json()
    addr = data.get("address", {}) or {}

    # city может лежать в разных ключах
    city = (
        addr.get("city")
        or addr.get("town")
        or addr.get("village")
        or addr.get("hamlet")
        or addr.get("municipality")
    )

    # region обычно state / region
    region = addr.get("state") or addr.get("region")

    return city, region

_TF = TimezoneFinder(in_memory=True)

def tz_from_moscow(latitude: float, longitude: float) -> Optional[int]:
    """
    Возвращает разницу в часах относительно Москвы:
    Тольятти -> 1, Калининград -> -1, Екатеринбург -> 2, ...
    """
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

def parse_location_data(input_link: str) -> dict:
    """
    Возвращает словарь данных, которые пойдут в general_location:
    name_point, link_point, latitude, longitude, distance_from_cremlin, city, region
    """

    input_link = (input_link or "").strip()
    if not input_link:
        raise ValueError("Пустая ссылка.")

    # 1) Нормализуем вход через твой link_handler:
    #    любая ссылка внутри парка -> главная
    main_link = link_handler.main_link_event(input_link)
    if not main_link:
        raise ValueError("Не удалось распознать ссылку парка (ожидаю https://5verst.ru/<slug>/...).")

    # 2) Качаем главную страницу и вытаскиваем name_point
    html_main = _fetch_html(main_link)
    if html_main is None:
        raise RuntimeError(f"Не удалось получить главную страницу парка: {main_link}")

    name_point = extract_name_point(html_main)
    if not name_point:
        raise ValueError("Не удалось извлечь name_point с главной страницы (div.text -> a).")

    # 3) Качаем course/ (если страницы нет -> берем главную для координат)
    course_link = link_handler.link_about_event(main_link)
    html_course = _fetch_html(course_link)  # может вернуть None при 404
    html_for_coords = html_course if html_course else html_main
    url_used = course_link if html_course else main_link

    soup = BeautifulSoup(html_for_coords, "html.parser")

    # 4) Координаты: пробуем набор экстракторов по приоритету
    latitude, longitude = (None, None)
    for extractor in (
        try_extract_coords_from_meta,
        try_extract_coords_from_meeting_point,
        try_extract_coords_from_text,
        try_extract_coords_from_yandex_links,
    ):
        latitude, longitude = extractor(soup)
        if latitude and longitude:
            break

    latitude = clean_coord(latitude)
    longitude = clean_coord(longitude)

    # 5) Расстояние от Кремля
    distance_from_cremlin = None
    if latitude and longitude:
        try:
            coordinate_cremlin = (55.7522, 37.6156)
            distance_from_cremlin = geodesic(
                coordinate_cremlin,
                (float(latitude), float(longitude))
            ).kilometers
        except Exception:
            distance_from_cremlin = None

    # 6) reverse geocode -> city/region
    city, region = (None, None)
    if latitude and longitude:
        try:
            city, region = reverse_geocode_city_region(float(latitude), float(longitude))
            region = normalize_region(region)  # убираем "область" и т.п.
            # не обязательно, но хорошо для Nominatim
            # (если ты добавляешь редко — можно убрать)
            sleep(1)
        except Exception:
            city, region = (None, None)

    tz_msk = None
    if latitude and longitude:
        tz_msk = tz_from_moscow(float(latitude), float(longitude))

    return {
        "name_point": name_point,
        "link_point": main_link,  # В БД кладём нормализованную главную ссылку
        "latitude": float(latitude) if latitude else None,
        "longitude": float(longitude) if longitude else None,
        "distance_from_cremlin": distance_from_cremlin,
        "city": city,
        "region": region,
        "tz_from_moscow": tz_msk,
        # полезный дебаг
        "debug_url_used": url_used,
        "debug_status": 200 if html_for_coords else None,
    }

def location_exists(engine, name_point: str) -> bool:
    q = sa.text("SELECT 1 FROM general_location WHERE name_point = :name_point LIMIT 1;")
    with engine.connect() as conn:
        res = conn.execute(q, {"name_point": name_point}).fetchone()
    return res is not None


def upsert_location(engine, row: dict) -> None:
    """
    Пишем прямо в general_location (включая link_point).

    ВАЖНО: предполагаем, что у general_location есть уникальность по name_point
    (UNIQUE или PK). Если её нет — лучше добавить.
    """
    q = sa.text("""
        INSERT INTO general_location (name_point, link_point, latitude, longitude, distance_from_cremlin, city, region, tz_from_moscow)
        VALUES (:name_point, :link_point, :latitude, :longitude, :distance_from_cremlin, :city, :region, :tz_from_moscow)
        ON CONFLICT (name_point) DO UPDATE SET
            link_point = EXCLUDED.link_point,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            distance_from_cremlin = EXCLUDED.distance_from_cremlin,
            city = EXCLUDED.city,
            region = EXCLUDED.region,
            tz_from_moscow = EXCLUDED.tz_from_moscow;
    """)
    with engine.begin() as conn:
        conn.execute(q, {
            "name_point": row["name_point"],
            "city": row.get("city"),
            "region": row.get("region"),
            "link_point": row["link_point"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "tz_from_moscow": row.get("tz_from_moscow"),
            "distance_from_cremlin": row["distance_from_cremlin"],
        })


def print_summary(row: dict, exists_before: bool) -> None:
    print("\n=== Сводка перед записью в БД ===")
    print(f"Локация (name_point): {row['name_point']}")
    print(f"Город (city):         {row.get('city')}")
    print(f"Регион (region):      {row.get('region')}")
    print(f"Ссылка (link_point):  {row['link_point']}")
    print(f"Широта:              {row['latitude']}")
    print(f"Долгота:             {row['longitude']}")
    print(f"TZ от Москвы (час):   {row.get('tz_from_moscow')}")
    print(f"Расстояние от Кремля (км): {row['distance_from_cremlin']}")
    print(f"Откуда парсили:       {row.get('debug_url_used')}")
    print(f"HTTP статус:          {row.get('debug_status')}")
    print(f"Уже была в БД:        {'ДА' if exists_before else 'НЕТ'}")
    print("==============================\n")


def add_location_by_link() -> None:
    credential = load_credential("5_verst.ini")
    engine = db_connect(credential)

    link = input("\nВставьте ссылку на страницу локации на 5verst.ru: ").strip()
    if not link:
        print("Пустая ссылка — выходим.")
        return

    try:
        row = parse_location_data(link)
    except Exception as e:
        print(f"❌ Не удалось распарсить локацию: {e}")
        return

    exists_before = location_exists(engine, row["name_point"])
    print_summary(row, exists_before)

    confirm = input("Записать это в БД? (1 — да, 0 — нет): ").strip()
    if confirm != "1":
        print("Отменено пользователем.")
        return

    try:
        upsert_location(engine, row)

        now = datetime.now()
        info_table_update(engine, "general_location", now)

        # refresh view (если она у тебя реально есть)
        try:
            update_view(engine, "new_turists")
        except Exception as e:
            print(f"⚠️ Не удалось обновить materialized view new_turists: {e}")

        print("✅ Готово: general_location обновлена.")
    except Exception as e:
        print(f"❌ Ошибка записи в БД: {e}")


if __name__ == "__main__":
    add_location_by_link()