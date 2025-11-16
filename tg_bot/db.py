import configparser
from datetime import datetime, timedelta
from typing import Optional

from dateutil.tz import gettz
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import NullPool
from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.types import JSON
from pathlib import Path
from typing import Union

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
    m = re.search(r'userstats/(\d+)/?', s)
    if m:
        return int(m.group(1))
    if re.fullmatch(r'\d+', s):
        return int(s)
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
