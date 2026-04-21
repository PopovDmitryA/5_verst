from sqlalchemy.orm import sessionmaker
import sqlalchemy as sa
import DB_handler as db
from telegram_notifier import send_telegram_notification, escape_markdown
from datetime import datetime
from .update_data_main import credential

def update_FIO():
    engine = db.db_connect(credential)
    Session = sessionmaker(bind=engine)
    session = Session()

    request_run = sa.text("""
    WITH users_with_multiple_names AS (
        SELECT user_id
        FROM (
            SELECT DISTINCT user_id, name_runner
            FROM (
                SELECT user_id, name_runner
                FROM details_protocol
                WHERE user_id IS NOT NULL
                UNION
                SELECT user_id, name_runner
                FROM details_vol
                WHERE user_id IS NOT NULL
            ) all_names
        ) grouped_by_user_and_name
        GROUP BY user_id
        HAVING COUNT(*) > 1
    ),
    all_user_names AS (
        SELECT
            dp.user_id,
            dp.name_runner,
            dp.name_point,
            dp.date_event,
            lae.last_check_at,
            'protocol' AS source_type
        FROM details_protocol dp
        JOIN list_all_events lae
          ON lae.name_point = dp.name_point
         AND lae.date_event = dp.date_event
        WHERE EXISTS (
            SELECT 1
            FROM users_with_multiple_names umn
            WHERE umn.user_id = dp.user_id
        )
        UNION ALL
        SELECT
            dv.user_id,
            dv.name_runner,
            dv.name_point,
            dv.date_event,
            lae.last_check_at,
            'vol' AS source_type
        FROM details_vol dv
        JOIN list_all_events lae
          ON lae.name_point = dv.name_point
         AND lae.date_event = dv.date_event
        WHERE EXISTS (
            SELECT 1
            FROM users_with_multiple_names umn
            WHERE umn.user_id = dv.user_id
        )
    ),
    ranked_names AS (
        SELECT
            user_id,
            name_runner,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY
                    last_check_at DESC NULLS LAST,
                    date_event DESC,
                    CASE WHEN source_type = 'protocol' THEN 1 ELSE 2 END
            ) AS rn
        FROM all_user_names
    ),
    latest_name AS (
        SELECT
            user_id,
            name_runner AS actual_name_runner
        FROM ranked_names
        WHERE rn = 1
    )
    UPDATE details_protocol
    SET name_runner = ln.actual_name_runner
    FROM latest_name ln
    WHERE details_protocol.user_id = ln.user_id
      AND details_protocol.name_runner != ln.actual_name_runner;
    """)
    result_run = session.execute(request_run)

    request_vol = sa.text("""
    WITH users_with_multiple_names AS (
        SELECT user_id
        FROM (
            SELECT DISTINCT user_id, name_runner
            FROM (
                SELECT user_id, name_runner
                FROM details_protocol
                WHERE user_id IS NOT NULL
                UNION
                SELECT user_id, name_runner
                FROM details_vol
                WHERE user_id IS NOT NULL
            ) all_names
        ) grouped_by_user_and_name
        GROUP BY user_id
        HAVING COUNT(*) > 1
    ),
    all_user_names AS (
        SELECT
            dp.user_id,
            dp.name_runner,
            dp.name_point,
            dp.date_event,
            lae.last_check_at,
            'protocol' AS source_type
        FROM details_protocol dp
        JOIN list_all_events lae
          ON lae.name_point = dp.name_point
         AND lae.date_event = dp.date_event
        WHERE EXISTS (
            SELECT 1
            FROM users_with_multiple_names umn
            WHERE umn.user_id = dp.user_id
        )
        UNION ALL
        SELECT
            dv.user_id,
            dv.name_runner,
            dv.name_point,
            dv.date_event,
            lae.last_check_at,
            'vol' AS source_type
        FROM details_vol dv
        JOIN list_all_events lae
          ON lae.name_point = dv.name_point
         AND lae.date_event = dv.date_event
        WHERE EXISTS (
            SELECT 1
            FROM users_with_multiple_names umn
            WHERE umn.user_id = dv.user_id
        )
    ),
    ranked_names AS (
        SELECT
            user_id,
            name_runner,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY
                    last_check_at DESC NULLS LAST,
                    date_event DESC,
                    CASE WHEN source_type = 'protocol' THEN 1 ELSE 2 END
            ) AS rn
        FROM all_user_names
    ),
    latest_name AS (
        SELECT
            user_id,
            name_runner AS actual_name_runner
        FROM ranked_names
        WHERE rn = 1
    )
    UPDATE details_vol
    SET name_runner = ln.actual_name_runner
    FROM latest_name ln
    WHERE details_vol.user_id = ln.user_id
      AND details_vol.name_runner != ln.actual_name_runner;
    """)
    result_vol = session.execute(request_vol)
    session.commit()

    count_run = result_run.rowcount if result_run.rowcount is not None else 0
    count_vol = result_vol.rowcount if result_vol.rowcount is not None else 0

    print(
        f'Обновлено {count_run} строк в таблице с бегунами\n'
        f'И {count_vol} строк в таблице с волонтёрами'
    )

    started_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if count_run == 0 and count_vol == 0:
        status_emoji = "⚪"
        message = (
            f"*__{status_emoji} update\\_FIO__*\n\n"
            f"*Время запуска:* {escape_markdown(started_at)}\n"
            f"*Обновлено строк в details\\_protocol:* {count_run}\n"
            f"*Обновлено строк в details\\_vol:* {count_vol}\n\n"
            f"Изменений в ФИО не найдено\\."
        )
    else:
        status_emoji = "🟢"
        message = (
            f"*__{status_emoji} update\\_FIO__*\n\n"
            f"*Время запуска:* {escape_markdown(started_at)}\n"
            f"*Обновлено строк в details\\_protocol:* {count_run}\n"
            f"*Обновлено строк в details\\_vol:* {count_vol}"
        )
    send_telegram_notification(message)

    session.close()

if __name__ == "__main__":
    print(f"{datetime.now()}: Запуск update_FIO")
    update_FIO()