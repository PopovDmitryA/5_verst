import time
import random
from update_data_functions import get_link_protocols_for_update, compare_and_update_single_protocol
from update_protocols import refresh_protocol_materialized_views
from .update_data_main import credential
from telegram_notifier import send_telegram_notification, escape_markdown
from datetime import datetime

def find_dif_details_protocol(count_last_protocol=3, name_point=None, oldest_first_limit=None):
    """
    Итерационно сверяет детали по протоколам.
    Каждый протокол:
    - скачивается отдельно
    - сразу сравнивается с БД
    - сразу при необходимости обновляется
    - после успешного завершения фиксируется last_check_at

    :param count_last_protocol: количество последних дат стартов для проверки
    :param name_point: список парков
    :param oldest_first_limit: если задан, выбрать N самых давно не проверявшихся протоколов
    """
    if name_point is None:
        name_point = []

    list_protocols = get_link_protocols_for_update(
        credential,
        count_last_protocol=count_last_protocol,
        name_point=name_point,
        oldest_first_limit=oldest_first_limit
    )

    print(f'{datetime.now()}: Выбрано {len(list_protocols)} протоколов для проверки')

    updated = 0
    no_changes = 0
    errors = 0
    updated_protocols = []

    total = len(list_protocols)

    for idx, (_, row) in enumerate(list_protocols.iterrows(), start=1):
        try:
            result = compare_and_update_single_protocol(
                credential,
                row,
                update_summary_row=False
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
            print(f'Ошибка при обработке протокола {row["name_point"]} / {row["date_event"]}: {e}')

        # Пауза между запросами к сайту, кроме последнего протокола
        if idx < total:
            delay = random.uniform(10, 20)
            print(f'Пауза {delay:.1f} сек перед следующим протоколом...')
            time.sleep(delay)

    if updated > 0:
        print('Обновляем materialized view после пачки изменений...')
        refresh_protocol_materialized_views(credential)
    print(f'''
Проверка завершена:
- обновлено протоколов: {updated}
- без изменений: {no_changes}
- с ошибками: {errors}
''')
    if oldest_first_limit is not None:
        mode_text = f"oldest_first_limit={oldest_first_limit}"
    else:
        mode_text = f"count_last_protocol={count_last_protocol}"

    park_text = "все парки"
    if name_point:
        park_text = f"парки: {', '.join(name_point)}"

    mode_text_escaped = escape_markdown(mode_text)
    park_text_escaped = escape_markdown(park_text)

    if errors > 0:
        status_emoji = "🟠"
    elif updated > 0:
        status_emoji = "🟢"
    else:
        status_emoji = "⚪"

    updated_list_text = ""
    if updated_protocols:
        updated_list_text = "\n\n*Обновлённые протоколы:*\n"
        for item in updated_protocols:
            updated_list_text += f"• {escape_markdown(item)}\n"

    if updated == 0:
        message = (
            f"*__{status_emoji} update\\_recent\\_by\\_count__*\n\n"
            f"*Режим:* {mode_text_escaped}\n"
            f"*Область:* {park_text_escaped}\n"
            f"*Проверено:* {len(list_protocols)}\n"
            f"*Обновлено:* {updated}\n"
            f"*Без изменений:* {no_changes}\n"
            f"*Ошибок:* {errors}\n\n"
            f"Изменений в протоколах не найдено\\."
        )
    else:
        message = (
            f"*__{status_emoji} update\\_recent\\_by\\_count__*\n\n"
            f"*Режим:* {mode_text_escaped}\n"
            f"*Область:* {park_text_escaped}\n"
            f"*Проверено:* {len(list_protocols)}\n"
            f"*Обновлено:* {updated}\n"
            f"*Без изменений:* {no_changes}\n"
            f"*Ошибок:* {errors}"
            f"{updated_list_text}"
        )

    send_telegram_notification(message)

if __name__ == "__main__":
    import sys
    import ast

    try:
        count = int(sys.argv[1]) if len(sys.argv) > 1 else None
        parks = ast.literal_eval(sys.argv[2]) if len(sys.argv) > 2 else None
        oldest_first_limit = int(sys.argv[3]) if len(sys.argv) > 3 else None

        if oldest_first_limit is not None:
            find_dif_details_protocol(
                count_last_protocol=count or 0,
                name_point=parks,
                oldest_first_limit=oldest_first_limit
            )
        elif count is not None and parks is not None:
            find_dif_details_protocol(count, parks)
        elif count is not None:
            find_dif_details_protocol(count)
        elif parks is not None:
            find_dif_details_protocol(0, parks)
        else:
            find_dif_details_protocol()

    except Exception as e:
        print(f"""
Ошибка запуска: {e}
Примеры:
  python -m schedule_scripts.update_recent_by_count 5
  python -m schedule_scripts.update_recent_by_count 5 "['Сосновка', 'Коломяги']"
  python -m schedule_scripts.update_recent_by_count 0 "[]" 100
  python -m schedule_scripts.update_recent_by_count
""")
