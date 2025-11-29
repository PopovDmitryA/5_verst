import sys
import re
import time
import configparser
from pathlib import Path
from datetime import datetime
from typing import Optional
from io import StringIO
import random

import pandas as pd
import sqlalchemy as sa
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# ---------- Настройки скрипта ----------

BASE_PARKRUN_URL = "https://www.parkrun.org.uk/parkrunner"

# ---------- Вспомогательные функции парсинга ----------

def fetch_two_pages_with_browser(browser, url_general: str, url_all: str):
    """
    Загружает две страницы в рамках ОДНОГО браузера:
      - /parkrunner/{id}/
      - /parkrunner/{id}/all/
    Возвращает html_general, html_all.
    """
    # страница 1 — general
    page_general = browser.new_page()
    page_general.goto(url_general, wait_until="networkidle")
    html_general = page_general.content()
    page_general.close()

    # страница 2 — all results
    page_all = browser.new_page()
    page_all.goto(url_all, wait_until="networkidle")
    html_all = page_all.content()
    page_all.close()

    return html_general, html_all


def parse_general_page(html: str):
    """
    Парсит общую страницу /parkrunner/{id}/
    Возвращает:
      - name_runner
      - age_category
      - df_vol (Volunteer Summary: Role / Occasions)
    """
    soup = BeautifulSoup(html, "html.parser")

    # ---- name_runner из <h2>Алексей САВЧУК <span>(A2278726)</span></h2>
    h2 = soup.find("h2")
    name_runner = None
    if h2:
        span = h2.find("span")
        if span:
            span.extract()
        name_runner = h2.get_text(strip=True)

    # ---- age_category: "Most recent age category was VM40-44"
    m = re.search(r"Most recent age category was\s+([A-Z0-9-]+)", html)
    age_category = m.group(1) if m else None

    # ---- Volunteer Summary (таблица role / occasions)
    vol_header = soup.find(
        lambda tag: tag.name in ["h2", "h3"] and "Volunteer Summary" in tag.get_text()
    )
    df_vol = None
    if vol_header:
        table = vol_header.find_next("table")
        if table:
            rows = []
            for tr in table.find_all("tr"):
                cols = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
                if cols:
                    rows.append(cols)

            if rows:
                header = rows[0]
                data = rows[1:]
                df_vol = pd.DataFrame(data, columns=header)

                # на всякий случай выбрасываем дублирующую строку заголовка
                mask_dup = (df_vol["Role"] == "Role") & (df_vol["Occasions"] == "Occasions")
                df_vol = df_vol[~mask_dup].reset_index(drop=True)

    return name_runner, age_category, df_vol


def parse_all_results_page(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")

    target_table = None
    for table in soup.find_all("table", class_="sortable"):
        caption = table.find("caption")
        if caption:
            text = caption.get_text(strip=True)
            if "All" in text and "Results" in text:
                target_table = table
                break

    if target_table is None:
        return pd.DataFrame()

    # ← исправление FutureWarning
    df = pd.read_html(StringIO(str(target_table)))[0]

    if "PB?" in df.columns:
        df["PB?"] = (
            df["PB?"]
            .astype(str)
            .str.replace("\xa0", "", regex=False)
            .str.strip()
            .replace({"": None, "nan": None})
        )

    return df

def is_captcha_html(html: str) -> bool:
    """
    Примитивная проверка на то, что страница превратилась в капчу / защитную заглушку.
    Никаких обходов, только детект и реакция (пауза).
    """
    if not html:
        return False
    text = html.lower()
    # типичные признаки:
    patterns = [
        "recaptcha",                     # гугловская капча
        "g-recaptcha",
        "are you a robot",               # классический текст
        "unusual traffic from your computer network",  # гугловская заглушка
        "to continue, please enable javascript",       # иногда на защитных страницах
        "our systems have detected",     # Google/Cloudflare style
        "/recaptcha/api.js",
    ]
    return any(p in text for p in patterns)

def _parse_time_to_hhmmss(val: str) -> Optional[str]:
    """
    Приводит строку вида '19:35' или '1:02:10' к формату 'HH:MM:SS'.
    Если не удаётся распарсить — возвращает None.
    """
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s:
        return None

    parts = s.split(":")
    try:
        if len(parts) == 2:
            h = 0
            m, sec = parts
        elif len(parts) == 3:
            h, m, sec = parts
        else:
            return None
        h = int(h)
        m = int(m)
        sec = int(sec)
        return f"{h:02d}:{m:02d}:{sec:02d}"
    except Exception:
        return None


def build_protocol_df(df_raw: pd.DataFrame, user_id: str, name_runner: str) -> pd.DataFrame:
    """
    Маппит таблицу All Results в структуру parkrun_details_protocol:
    name_point, date_event, name_runner, user_id,
    index_event, position, finish_time, age_grade, pr
    """
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(
            columns=[
                "name_point",
                "date_event",
                "name_runner",
                "user_id",
                "index_event",
                "position",
                "finish_time",
                "age_grade",
                "pr",
            ]
        )

    df = df_raw.copy()

    df = df.rename(
        columns={
            "Event": "name_point",
            "Run Date": "date_event",
            "Run Number": "index_event",
            "Pos": "position",
            "Time": "finish_time_raw",
            "Age Grade": "age_grade",
            "PB?": "pr_raw",
        }
    )

    # Дата: dd/mm/yyyy -> timestamp (dayfirst=True важно!)
    df["date_event"] = pd.to_datetime(df["date_event"], dayfirst=True, errors="coerce")

    # Время финиша -> строка HH:MM:SS
    df["finish_time"] = df["finish_time_raw"].apply(_parse_time_to_hhmmss)

    # PR: PB -> "РВ", остальное/NaN -> None
    df["pr"] = df["pr_raw"].replace({"PB": "РВ"}).where(df["pr_raw"].notna(), None)

    # Числовые поля
    df["index_event"] = pd.to_numeric(df["index_event"], errors="coerce").astype("Int64")
    df["position"] = pd.to_numeric(df["position"], errors="coerce").astype("Int64")

    # user_id и name_runner
    df["user_id"] = str(user_id)
    df["name_runner"] = name_runner

    cols = [
        "name_point",
        "date_event",
        "name_runner",
        "user_id",
        "index_event",
        "position",
        "finish_time",
        "age_grade",
        "pr",
    ]
    return df[cols]


def build_vol_summary_df(df_vol: pd.DataFrame, user_id: str, name_runner: str) -> pd.DataFrame:
    """
    Маппит Volunteer Summary в структуру parkrun_vol_summary:
    user_id, name_runner, vol_role, count_vol
    """
    if df_vol is None or df_vol.empty:
        return pd.DataFrame(columns=["user_id", "name_runner", "vol_role", "count_vol"])

    df = df_vol.copy()
    df = df.rename(columns={"Role": "vol_role", "Occasions": "count_vol"})
    df["count_vol"] = pd.to_numeric(df["count_vol"], errors="coerce").astype("Int64")
    df["user_id"] = str(user_id)
    df["name_runner"] = name_runner

    return df[["user_id", "name_runner", "vol_role", "count_vol"]]


def normalize_parkrun_id(user_id: str) -> str:
    """
    Для URL нужен числовой ID.
    Если в БД хранится 'A2278726', превращаем в '2278726'.
    """
    return str(user_id).lstrip("A")


# ---------- Основная логика ----------

def main():
    # --- пути и config.ini ---
    current_dir = Path(__file__).resolve().parent
    project_root = current_dir.parent
    config_path = project_root / "5_verst.ini"

    # чтобы подтянуть DB_handler с уровнем выше
    sys.path.append(str(project_root))
    from DB_handler import db_connect  # type: ignore

    config = configparser.ConfigParser()
    read_files = config.read(config_path, encoding="utf-8")

    if not read_files:
        raise FileNotFoundError(f"Не найден конфиг: {config_path}")

    db_host = config["five_verst_stats"]["host"]
    db_user = config["five_verst_stats"]["username"]
    db_pass = config["five_verst_stats"]["password"]
    db_name = config["five_verst_stats"]["dbname"]

    credential = f"postgresql://{db_user}:{db_pass}@{db_host}/{db_name}"
    engine = db_connect(credential)

    parser_cfg = config["parkrun_parser"]

    MAX_USERS_PER_RUN = parser_cfg.getint("max_users_per_run")
    MIN_SLEEP_BETWEEN_USERS = parser_cfg.getint("min_sleep_between_users")
    MAX_SLEEP_BETWEEN_USERS = parser_cfg.getint("max_sleep_between_users")

    USERS_PER_BROWSER_SESSION = parser_cfg.getint("users_per_browser_session")

    HEADLESS = parser_cfg.getboolean("headless")

    backoff_schedule_minutes = [
        parser_cfg.getint("captcha_backoff_1"),
        parser_cfg.getint("captcha_backoff_2"),
        parser_cfg.getint("captcha_backoff_3")
    ]

    processed = 0
    captcha_attempts = 0  # сколько раз подряд словили капчу

    # через сколько пользователей перезапускать браузер
    # USERS_PER_BROWSER_SESSION = 10
    users_in_current_browser = 0

    # backoff_schedule_minutes = [5, 10, 20]  # 1-я, 2-я, 3-я попытка капчи

    from playwright.sync_api import Error as PlaywrightError

    with sync_playwright() as p:
        browser = None

        def launch_browser():
            """Локальная функция для запуска браузера с нужными аргументами."""
            return p.chromium.launch(
                headless=HEADLESS,
                args=[
                    "--window-position=-2000,-2000",
                    "--window-size=10,10",
                ] if not HEADLESS else []
            )

        while processed < MAX_USERS_PER_RUN:
            # если браузера ещё нет или мы уже обработали в нём N пользователей — перезапускаем
            if browser is None or users_in_current_browser >= USERS_PER_BROWSER_SESSION:
                if browser is not None:
                    try:
                        browser.close()
                    except Exception:
                        pass
                browser = launch_browser()
                users_in_current_browser = 0
                print("\n🔁 Запущен новый экземпляр браузера")

            # --- берём одного юзера с last_updated IS NULL ---
            with engine.connect() as conn:
                result = conn.execute(
                    sa.text(
                        """
                        SELECT user_id
                        FROM parkrun_users
                        WHERE last_updated IS NULL
                        ORDER BY user_id
                        LIMIT 1
                        """
                    )
                )
                row = result.fetchone()

            if row is None:
                print("Больше нет пользователей с last_updated IS NULL — выходим.")
                break

            user_id = row[0]
            print(f"\n=== Обрабатываем user_id = {user_id} ===")

            parkrun_numeric_id = normalize_parkrun_id(user_id)
            url_general = f"{BASE_PARKRUN_URL}/{parkrun_numeric_id}/"
            url_all = f"{BASE_PARKRUN_URL}/{parkrun_numeric_id}/all/"

            while True:
                try:
                    html_general, html_all = fetch_two_pages_with_browser(
                        browser, url_general, url_all
                    )
                except PlaywrightError as e:
                    print(f"❌ Playwright ошибка при загрузке user_id={user_id}: {e}", file=sys.stderr)
                    # перезапускаем браузер и помечаем пользователя как проблемного (оставим last_updated=NULL)
                    try:
                        browser.close()
                    except Exception:
                        pass
                    browser = launch_browser()
                    users_in_current_browser = 0
                    break  # выходим из while True для этого юзера

                # --- детектор капчи ---
                if is_captcha_html(html_general) or is_captcha_html(html_all):
                    captcha_attempts += 1
                    print(f"⚠ Обнаружена капча (попытка #{captcha_attempts}).")

                    # закрываем браузер
                    try:
                        browser.close()
                    except Exception:
                        pass
                    browser = None
                    users_in_current_browser = 0

                    if captcha_attempts > len(backoff_schedule_minutes):
                        print("❌ Капча не проходит после 3 попыток, завершаем работу скрипта.")
                        return

                    backoff_minutes = backoff_schedule_minutes[captcha_attempts - 1]
                    print(f"⏸ Пауза на {backoff_minutes} минут(ы) перед повторной попыткой...")
                    time.sleep(backoff_minutes * 60)

                    # после паузы цикл while продолжится, и в начале while создастся новый браузер
                    continue

                # если сюда дошли – капчи нет, сбрасываем счётчик
                captcha_attempts = 0

                # --- обычный парсинг ---
                try:
                    name_runner, age_category, df_vol_raw = parse_general_page(html_general)
                    df_results_raw = parse_all_results_page(html_all)

                    if name_runner is None:
                        print(
                            f"❗ Не удалось распарсить name_runner для user_id={user_id}. Помечаем как проблемного и пропускаем.")

                        # записываем "технический" last_updated
                        with engine.begin() as conn:
                            conn.execute(
                                sa.text(
                                    """
                                    UPDATE parkrun_users
                                    SET last_updated = :ts
                                    WHERE user_id = :user_id
                                    """
                                ),
                                {
                                    "ts": datetime(1990, 1, 1),
                                    "user_id": str(user_id),
                                },
                            )

                        # после этого пользователь будет пропущен при следующем SELECT
                        # увеличим счётчик пользователей, чтобы не сбить логику сессий
                        processed += 1
                        users_in_current_browser += 1

                        # прекращаем обработку текущего user_id
                        break

                    df_protocol = build_protocol_df(
                        df_results_raw, user_id=str(user_id), name_runner=name_runner
                    )
                    df_vol_summary = build_vol_summary_df(
                        df_vol_raw, user_id=str(user_id), name_runner=name_runner
                    )

                    # --- всё в БД в одной транзакции ---
                    with engine.begin() as conn:
                        # 1) обновляем имя и категорию
                        conn.execute(
                            sa.text(
                                """
                                UPDATE parkrun_users
                                SET actual_name_runner = :name_runner,
                                    actual_age_category = :age_category
                                WHERE user_id = :user_id
                                """
                            ),
                            {
                                "name_runner": name_runner,
                                "age_category": age_category,
                                "user_id": str(user_id),
                            },
                        )

                        # 2) протоколы
                        if not df_protocol.empty:
                            df_protocol.to_sql(
                                "parkrun_details_protocol",
                                con=conn,
                                if_exists="append",
                                index=False,
                            )

                        # 3) волонтёрский summary
                        if not df_vol_summary.empty:
                            df_vol_summary.to_sql(
                                "parkrun_vol_summary",
                                con=conn,
                                if_exists="append",
                                index=False,
                            )

                        # 4) last_updated
                        conn.execute(
                            sa.text(
                                """
                                UPDATE parkrun_users
                                SET last_updated = :ts
                                WHERE user_id = :user_id
                                """
                            ),
                            {
                                "ts": datetime.now(),
                                "user_id": str(user_id),
                            },
                        )

                    processed += 1
                    users_in_current_browser += 1

                    print(f"✅ Готово по user_id={user_id}. Всего обработано: {processed}")

                except Exception as e:
                    print(f"❌ Ошибка при обработке user_id={user_id}: {e}", file=sys.stderr)

                # выходим из while True для этого user_id (либо после успеха, либо после ошибки)
                break

            if processed >= MAX_USERS_PER_RUN:
                break

            # случайная пауза между пользователями
            sleep_sec = random.uniform(MIN_SLEEP_BETWEEN_USERS, MAX_SLEEP_BETWEEN_USERS)
            print(f"⏳ Пауза между пользователями: {sleep_sec:.1f} сек.")
            time.sleep(sleep_sec)

        if browser is not None:
            try:
                browser.close()
            except Exception:
                pass

    print(f"\nЗавершено. Обработано пользователей за запуск: {processed}")

if __name__ == "__main__":
    main()
