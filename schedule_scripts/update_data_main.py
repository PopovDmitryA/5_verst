import configparser
from update_data_functions import create_list_for_compare
from update_protocols import refresh_protocol_materialized_views
from pathlib import Path
import time
import random

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
CONFIG_PATH = PROJECT_ROOT / "5_verst.ini"

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

db_host = config['five_verst_stats']['host']
db_user = config['five_verst_stats']['username']
db_pass = config['five_verst_stats']['password']
db_name = config['five_verst_stats']['dbname']

credential = f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}'

def func_update_protocols(different_list_of_protocols):
    """
    Итерационная обработка протоколов, найденных по отличиям в саммари.
    Каждый протокол:
    - парсится отдельно
    - сравнивается отдельно
    - обновляется отдельно
    - после успеха получает новый last_check_at
    """
    from update_data_functions import compare_and_update_single_protocol

    print(f'Нашли {len(different_list_of_protocols)} протоколов с отличиями')

    updated = 0
    no_changes = 0
    errors = 0
    updated_protocols = []

    total = len(different_list_of_protocols)

    for idx, (_, row) in enumerate(different_list_of_protocols.iterrows(), start=1):
        try:
            result = compare_and_update_single_protocol(
                credential,
                row,
                update_summary_row=True
            )

            if result["status"] == "updated":
                updated += 1
                updated_protocols.append(
                    f"{row['name_point']} — {row['date_event'].strftime('%Y-%m-%d')}"
                )
            else:
                no_changes += 1

        except Exception as e:
            errors += 1
            print(f'Ошибка при обработке {row["name_point"]} / {row["date_event"]}: {e}')

        # Пауза между запросами к сайту, кроме последнего протокола
        if idx < total:
            delay = random.uniform(10, 20)
            print(f'Пауза {delay:.1f} сек перед следующим протоколом...')
            time.sleep(delay)

    if updated > 0:
        print('Обновляем materialized view после пачки изменений...')
        refresh_protocol_materialized_views(credential)
    print(f'''
Обработка завершена:
Обновлено: {updated}
Без изменений: {no_changes}
Ошибок: {errors}
''')
    return {
        "updated": updated,
        "no_changes": no_changes,
        "errors": errors,
        "total": len(different_list_of_protocols),
        "updated_protocols": updated_protocols,
    }

def list_point_update():
    return create_list_for_compare(credential)
