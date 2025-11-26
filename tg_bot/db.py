import configparser
import re
from datetime import datetime, timedelta
from typing import Optional, Union

from dateutil.tz import gettz
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import NullPool
from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.types import JSON
from pathlib import Path

TZ = gettz("Europe/Moscow")

# ===== Подключение к БД =====
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
CONFIG_PATH = PROJECT_ROOT / "5_verst.ini"

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

db_host = config['five_verst_stats']['host']
db_user = config['five_verst_stats']['username']
db_pass = config['five_verst_stats']['password']
db_name = config['five_verst_stats']['dbname']

credential = f'postgresql+psycopg://{db_user}:{db_pass}@{db_host}/{db_name}'
engine: Engine = create_engine(credential, poolclass=NullPool, future=True)


# ===== Утилиты =====
def uid_str(uid: Union[int, str]) -> str:
    return str(uid)

def log_action(tg_user_id: int, action: str, success: bool, details: dict):
    stmt = text("""
        INSERT INTO change_log (tg_user_id, action, success, details)
        VALUES (:tg, :act, :succ, :det)
    """).bindparams(bindparam("det", type_=JSON))  # пусть SQLAlchemy сам сериализует в JSONB

    with engine.begin() as conn:
        conn.execute(
            stmt,
            {"tg": tg_user_id, "act": action, "succ": success, "det": details}
        )

def ensure_user_row(tg_user_id: int, tg_username: Optional[str], tg_chat_id: Optional[int]):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO tg_user_profile (tg_user_id, tg_username, tg_chat_id)
            VALUES (:id, :un, :chat)
            ON CONFLICT (tg_user_id) DO UPDATE SET
                tg_username = EXCLUDED.tg_username,
                tg_chat_id  = EXCLUDED.tg_chat_id
        """), {"id": tg_user_id, "un": tg_username, "chat": tg_chat_id})

def get_profile(tg_user_id: int):
    with engine.begin() as conn:
        row = conn.execute(text("SELECT * FROM tg_user_profile WHERE tg_user_id=:u"),
                           {"u": tg_user_id}).mappings().first()
    return row

def set_consent(tg_user_id: int, accepted: bool):
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE tg_user_profile
            SET consent_accepted=:acc, consent_ts=now()
            WHERE tg_user_id=:u
        """), {"acc": accepted, "u": tg_user_id})

def set_news_subscribed(tg_user_id: int, subscribed: bool):
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE tg_user_profile
            SET news_subscribed = :sub
            WHERE tg_user_id = :u
        """), {"sub": subscribed, "u": tg_user_id})

def get_news_subscribed_tg_ids() -> list[int]:
    """
    Возвращает список tg_user_id пользователей,
    у которых news_subscribed = true.
    """
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT tg_user_id
            FROM tg_user_profile
            WHERE news_subscribed = true
        """)).fetchall()
    return [r[0] for r in rows]

def next_time_after(last_dt) -> Optional[datetime]:
    if not last_dt:
        return None
    return last_dt + timedelta(hours=24)

def can_change(field_name: str, tg_user_id: int):
    row = get_profile(tg_user_id)
    last = row[field_name] if row and field_name in row else None
    if not last:
        return True, None
    nt = next_time_after(last)
    now = datetime.now(TZ)
    return (now >= nt), nt

def parse_user_id_from_text(s: str) -> Optional[int]:
    s = s.strip()
    import re

    # 1) Пытаемся вытащить ID из ссылки вида .../userstats/12345/
    m = re.search(r'userstats/(\d+)/?', s)
    if m:
        return int(m.group(1))

    # 2) Вытаскиваем любые цифры, даже если есть буква, например A79352523
    m = re.search(r'(\d+)', s)
    if m:
        return int(m.group(1))

    return None

def find_latest_name_for_user(uid: int) -> Optional[str]:
    sql = """
    WITH last_runs AS (
        SELECT name_runner, date_event
        FROM details_protocol
        WHERE user_id = CAST(:uid AS TEXT)
        ORDER BY date_event DESC
        LIMIT 1
    ),
    last_vols AS (
        SELECT name_runner, date_event
        FROM details_vol
        WHERE user_id = CAST(:uid AS TEXT)
        ORDER BY date_event DESC
        LIMIT 1
    )
    SELECT name_runner FROM (
        SELECT * FROM last_runs
        UNION ALL
        SELECT * FROM last_vols
    ) t
    ORDER BY date_event DESC
    LIMIT 1;
    """
    with engine.begin() as conn:
        row = conn.execute(text(sql), {"uid": uid}).first()
    return row[0] if row else None

def user_exists(uid: int) -> bool:
    sql = """
    SELECT
        EXISTS (SELECT 1 FROM details_protocol WHERE user_id = CAST(:u AS TEXT))
        OR
        EXISTS (SELECT 1 FROM details_vol      WHERE user_id = CAST(:u AS TEXT))
    """
    with engine.begin() as conn:
        val = conn.execute(text(sql), {"u": uid}).scalar()
    return bool(val)

def is_5v_profile_bound(uid: Union[int, str], tg_user_id: int) -> bool:
    """
    Проверяем, что данный user_id_5v уже привязан к другой TG-учётке.

    user_id_5v в tg_user_profile хранится как text, поэтому приводим к str.
    """
    u_str = str(uid)
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT tg_user_id
                FROM tg_user_profile
                WHERE user_id_5v = :uid
                  AND tg_user_id <> :tg
                LIMIT 1
            """),
            {"uid": u_str, "tg": tg_user_id},
        ).first()
    return row is not None



def is_parkrun_profile_bound(parkrun_user_id: Union[int, str], tg_user_id: int) -> bool:
    pid = str(parkrun_user_id)
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT tg_user_id
                FROM tg_user_profile
                WHERE parkrun_user_id = :pid
                  AND tg_user_id <> :tg
                LIMIT 1
            """),
            {"pid": pid, "tg": tg_user_id},
        ).first()
    return row is not None


def is_s95_profile_bound(s95_id: str, tg_user_id: int) -> bool:
    sid = str(s95_id)
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT tg_user_id
                FROM tg_user_profile
                WHERE s95_user_id = :sid
                  AND tg_user_id <> :tg
                LIMIT 1
            """),
            {"sid": sid, "tg": tg_user_id},
        ).first()
    return row is not None

def bind_profile(tg_user_id: int, uid: int, profile_url: str) -> tuple[bool, str]:
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE tg_user_profile
                SET user_id_5v=:uid, profile_url=:url, bound_at=now(), last_profile_change_at=now()
                WHERE tg_user_id=:tg
            """), {"uid": uid, "url": profile_url, "tg": tg_user_id})
        return True, "ok"
    except IntegrityError as e:
        # нарушена уникальность user_id_5v (он уже привязан к другому TG)
        return False, "Этот профиль уже привязан к другому Telegram-аккаунту."

def list_clubs_distinct() -> list[str]:
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT DISTINCT club FROM list_clubs WHERE club IS NOT NULL ORDER BY club")).all()
    return [r[0] for r in rows]

def get_current_club(uid: Union[int, str]) -> Optional[str]:
    u = uid_str(uid)
    with engine.begin() as conn:
        row = conn.execute(text("SELECT club FROM list_clubs WHERE user_id = :u"), {"u": u}).first()
    return row[0] if row else None

def set_user_club(tg_user_id: int, uid: Union[int, str], club: str):
    u = uid_str(uid)
    prev = get_current_club(u)
    with engine.begin() as conn:
        # user_id в list_clubs текстовый, поэтому передаём строку
        conn.execute(
            text("""INSERT INTO list_clubs (user_id, club) VALUES (:u, :c)
                    ON CONFLICT (user_id) DO UPDATE SET club = EXCLUDED.club"""),
            {"u": u, "c": club}
        )
        conn.execute(
            text("""INSERT INTO club_change_log (tg_user_id, user_id_5v, from_club, to_club)
                    VALUES (:tg, :u_num, :f, :t)"""),
            # В club_change_log.user_id_5v у вас тип BIGINT? Если да — передаём числом.
            {"tg": tg_user_id, "u_num": int(uid), "f": prev, "t": club}
        )
        conn.execute(text("UPDATE tg_user_profile SET last_club_change_at=now() WHERE tg_user_id=:tg"),
                     {"tg": tg_user_id})

def delete_user_club(tg_user_id: int, uid: Union[int, str]) -> bool:
    u = uid_str(uid)
    prev = get_current_club(u)
    with engine.begin() as conn:
        res = conn.execute(text("DELETE FROM list_clubs WHERE user_id = :u"), {"u": u})
        if res.rowcount:
            conn.execute(
                text("""INSERT INTO club_change_log (tg_user_id, user_id_5v, from_club, to_club, note)
                        VALUES (:tg, :u_num, :f, NULL, 'delete')"""),
                {"tg": tg_user_id, "u_num": int(uid), "f": prev}
            )
            conn.execute(text("UPDATE tg_user_profile SET last_club_change_at=now() WHERE tg_user_id=:tg"),
                         {"tg": tg_user_id})
            return True
    return False

def unlink_profile(tg_user_id: int) -> bool:
    """
    Отвязывает профиль 5в от TG-учётки (ставим NULL'ы) и фиксируем время изменения.
    Возвращает True, если что-то реально поменяли.
    """
    with engine.begin() as conn:
        res = conn.execute(
            text("""
                UPDATE tg_user_profile
                SET user_id_5v = NULL,
                    profile_url = NULL,
                    bound_at = NULL,
                    last_profile_change_at = now()
                WHERE tg_user_id = :tg AND user_id_5v IS NOT NULL
            """),
            {"tg": tg_user_id}
        )
    return res.rowcount > 0

def mark_first_start(tg_user_id: int):
    """
    Фиксирует время первого /start для пользователя.
    Если значение уже есть, ничего не меняет.
    """
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE tg_user_profile
                SET first_start_ts = now()
                WHERE tg_user_id = :u AND first_start_ts IS NULL
            """),
            {"u": tg_user_id}
        )

def get_parkrun_user(user_id: int):
    """
    Ищем пользователя в parkrun_users по user_id.
    """
    sql = """
        SELECT user_id, actual_name_runner, name_runner
        FROM parkrun_users
        WHERE user_id = CAST(:uid AS TEXT)
    """
    with engine.begin() as conn:
        row = conn.execute(text(sql), {"uid": user_id}).mappings().first()
    return row

def ensure_parkrun_user_row(user_id: str):
    """
    Обеспечивает, что в parkrun_users есть строка с этим user_id.
    Если уже есть — ничего не делает.
    """
    sql = """
        INSERT INTO parkrun_users (user_id)
        SELECT CAST(:uid AS TEXT)
        WHERE NOT EXISTS (
            SELECT 1 FROM parkrun_users WHERE user_id = CAST(:uid AS TEXT)
        )
    """
    with engine.begin() as conn:
        conn.execute(text(sql), {"uid": user_id})

def bind_parkrun_profile(tg_user_id: int, parkrun_user_id: str):
    """
    Привязываем parkrun_user_id к tg_user_profile и фиксируем время изменения.
    """
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE tg_user_profile
            SET parkrun_user_id = :pid,
                last_parkrun_change_at = now()
            WHERE tg_user_id = :tg
        """), {"pid": parkrun_user_id, "tg": tg_user_id})

def unlink_parkrun_profile(tg_user_id: int) -> bool:
    """
    Отвязываем parkrun_user_id от tg_user_profile и фиксируем время изменения.
    Возвращаем True, если что-то реально изменилось.
    """
    with engine.begin() as conn:
        res = conn.execute(text("""
            UPDATE tg_user_profile
            SET parkrun_user_id = NULL,
                last_parkrun_change_at = now()
            WHERE tg_user_id = :tg AND parkrun_user_id IS NOT NULL
        """), {"tg": tg_user_id})
    return res.rowcount > 0

def _digits_only(value: str) -> str:
    """
    Вспомогательная функция: оставляем только цифры.
    Используем для поиска по S95 ID / barcode.
    """
    if value is None:
        return ""
    return re.sub(r"\D", "", str(value))


def get_s95_runner(value: str):
    """
    Ищем участника в s95_runners по s95_id ИЛИ по s95_barcode.
    Везде работаем только с цифрами (строкой).
    """
    digits = _digits_only(value)
    if not digits:
        return None

    sql = """
        SELECT s95_id, s95_barcode, name_runner
        FROM s95_runners
        WHERE s95_id = :val OR s95_barcode = :val
        LIMIT 1
    """
    with engine.begin() as conn:
        row = conn.execute(text(sql), {"val": digits}).mappings().first()
    return row


def get_s95_by_barcode(barcode: str):
    """
    Ищем участника в s95_runners по штрихкоду (s95_barcode).
    Всегда работаем только с цифрами.
    """
    digits = _digits_only(barcode)
    if not digits:
        return None

    sql = """
        SELECT s95_id, s95_barcode, name_runner
        FROM s95_runners
        WHERE s95_barcode = :val
        LIMIT 1
    """
    with engine.begin() as conn:
        row = conn.execute(text(sql), {"val": digits}).mappings().first()
    return row

def ensure_s95_runner_row(s95_id: str):
    """
    Обеспечивает наличие строки с данным s95_id в s95_runners.
    Если строки нет — создаём пустую с одним s95_id.
    """
    sql = """
        INSERT INTO s95_runners (s95_id)
        SELECT :sid
        WHERE NOT EXISTS (
            SELECT 1 FROM s95_runners
            WHERE s95_id = :sid
        )
    """
    with engine.begin() as conn:
        conn.execute(text(sql), {"sid": s95_id})


def bind_s95_profile(tg_user_id: int, s95_id: str) -> tuple[bool, str]:
    """
    Привязываем s95_id к tg_user_profile.s95_user_id.
    Фиксируем last_s95_change_at.
    """
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE tg_user_profile
                SET s95_user_id = :sid,
                    last_s95_change_at = now()
                WHERE tg_user_id = :tg
            """), {"sid": s95_id, "tg": tg_user_id})
        return True, "ok"
    except IntegrityError:
        # нарушена уникальность s95_user_id (уже привязан к другому TG)
        return False, "Этот профиль С95 уже привязан к другому Telegram-аккаунту."


def unlink_s95_profile(tg_user_id: int) -> bool:
    """
    Отвязываем s95_user_id от tg_user_profile и фиксируем время изменения.
    Возвращаем True, если что-то реально изменилось.
    """
    with engine.begin() as conn:
        res = conn.execute(text("""
            UPDATE tg_user_profile
            SET s95_user_id = NULL,
                last_s95_change_at = now()
            WHERE tg_user_id = :tg AND s95_user_id IS NOT NULL
        """), {"tg": tg_user_id})
    return res.rowcount > 0

def get_5v_runs_count(user_id: Union[int, str]) -> int:
    """
    Количество пробежек 5 вёрст (без тестовых стартов).
    """
    u = uid_str(user_id)
    sql = """
        SELECT count(*) AS cnt
        FROM details_protocol dp
        JOIN list_all_events e USING (name_point, date_event)
        WHERE dp.user_id = :uid
          AND e.is_test = false
    """
    with engine.begin() as conn:
        row = conn.execute(text(sql), {"uid": u}).first()
    return row[0] if row else 0


def get_parkrun_runs_count(user_id: Union[int, str]) -> int:
    """
    Количество пробежек parkrun.
    """
    u = uid_str(user_id)
    sql = """
        SELECT count(*) AS cnt
        FROM parkrun_details_protocol pdp
        WHERE pdp.user_id = :uid
    """
    with engine.begin() as conn:
        row = conn.execute(text(sql), {"uid": u}).first()
    return row[0] if row else 0


def get_s95_runs_count(user_id: Union[int, str]) -> int:
    """
    Количество пробежек в системе С95.
    """
    u = uid_str(user_id)
    sql = """
        SELECT count(*) AS cnt
        FROM s95_details_protocol sdp
        WHERE sdp.user_id = :uid
    """
    with engine.begin() as conn:
        row = conn.execute(text(sql), {"uid": u}).first()
    return row[0] if row else 0

def get_last_5v_run(user_id: Union[int, str]):
    u = uid_str(user_id)
    sql = """
        SELECT e.date_event, e.name_point
        FROM details_protocol dp
        JOIN list_all_events e USING (name_point, date_event)
        WHERE dp.user_id = :uid
          AND e.is_test = false
        ORDER BY e.date_event DESC
        LIMIT 1
    """
    with engine.begin() as conn:
        row = conn.execute(text(sql), {"uid": u}).first()
    if not row:
        return None
    return {"date_event": row[0], "name_point": row[1]}


def get_last_parkrun_run(user_id: Union[int, str]):
    u = uid_str(user_id)
    sql = """
        SELECT date_event, name_point
        FROM parkrun_details_protocol
        WHERE user_id = :uid
        ORDER BY date_event DESC
        LIMIT 1
    """
    with engine.begin() as conn:
        row = conn.execute(text(sql), {"uid": u}).first()
    if not row:
        return None
    return {"date_event": row[0], "name_point": row[1]}


def get_last_s95_run(user_id: Union[int, str]):
    """
    Возвращает дату и локацию последней пробежки в системе С95
    для данного user_id (по таблице s95_details_protocol).
    """
    u = uid_str(user_id)
    sql = """
        SELECT date_event, name_point
        FROM s95_details_protocol
        WHERE user_id = :uid
        ORDER BY date_event DESC
        LIMIT 1
    """
    with engine.begin() as conn:
        row = conn.execute(text(sql), {"uid": u}).first()
    if not row:
        return None
    return {
        "date_event": row[0],
        "name_point": row[1],
    }
