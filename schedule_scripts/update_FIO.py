from sqlalchemy.orm import sessionmaker
import sqlalchemy as sa
from .update_data_main import credential

def update_FIO():
    engine = sa.create_engine(credential)
    Session = sessionmaker(bind=engine)
    session = Session()

    request_run = sa.text(f"""
WITH users_with_multiple_names AS (
    SELECT user_id
    FROM (
        SELECT DISTINCT user_id, name_runner
        FROM (
            SELECT user_id, name_runner
            FROM details_protocol
            UNION ALL
            SELECT user_id, name_runner
            FROM details_vol
        ) all_names
    ) grouped_by_user_and_name
    GROUP BY user_id
    HAVING COUNT(*) > 1
),
latest_name_per_user AS (
    SELECT user_id,
           MAX(date_event) AS max_date_event
    FROM (
        SELECT user_id, date_event
        FROM details_protocol
        WHERE EXISTS (SELECT 1 FROM users_with_multiple_names umn WHERE umn.user_id = details_protocol.user_id)
        UNION ALL
        SELECT user_id, date_event
        FROM details_vol
        WHERE EXISTS (SELECT 1 FROM users_with_multiple_names umn WHERE umn.user_id = details_vol.user_id)
    ) all_events
    GROUP BY user_id
),
latest_name AS (
    SELECT lp.user_id,
           COALESCE(dv.name_runner, dp.name_runner) AS actual_name_runner
    FROM latest_name_per_user lp
    LEFT JOIN details_protocol dp ON lp.user_id = dp.user_id AND lp.max_date_event = dp.date_event
    LEFT JOIN details_vol dv ON lp.user_id = dv.user_id AND lp.max_date_event = dv.date_event
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
            UNION ALL
            SELECT user_id, name_runner
            FROM details_vol
        ) all_names
    ) grouped_by_user_and_name
    GROUP BY user_id
    HAVING COUNT(*) > 1
),
latest_name_per_user AS (
    SELECT user_id,
           MAX(date_event) AS max_date_event
    FROM (
        SELECT user_id, date_event
        FROM details_protocol
        WHERE EXISTS (SELECT 1 FROM users_with_multiple_names umn WHERE umn.user_id = details_protocol.user_id)
        UNION ALL
        SELECT user_id, date_event
        FROM details_vol
        WHERE EXISTS (SELECT 1 FROM users_with_multiple_names umn WHERE umn.user_id = details_vol.user_id)
    ) all_events
    GROUP BY user_id
),
latest_name AS (
    SELECT lp.user_id,
           COALESCE(dv.name_runner, dp.name_runner) AS actual_name_runner
    FROM latest_name_per_user lp
    LEFT JOIN details_protocol dp ON lp.user_id = dp.user_id AND lp.max_date_event = dp.date_event
    LEFT JOIN details_vol dv ON lp.user_id = dv.user_id AND lp.max_date_event = dv.date_event
)
UPDATE details_vol
SET name_runner = ln.actual_name_runner
FROM latest_name ln
WHERE details_vol.user_id = ln.user_id
AND details_vol.name_runner != ln.actual_name_runner;
""")
    result_vol = session.execute(request_vol)
    session.commit()
    print(f'Обновлено {result_run.rowcount} строк в таблице с бегунами\nИ {result_vol.rowcount} строк в таблице с волонтёрами')
    session.close()