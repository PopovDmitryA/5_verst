import update_data_functions as udf
from .update_data_main import func_update_protocols, credential
from telegram_notifier import send_telegram_notification, escape_markdown
from datetime import datetime


def record_latest_protocol():
    """Записываем новые пробежки и обновляем те, которые подверглись изменениям
    в саммари со страницы последних забегов.
    """
    started_at = datetime.now()
    print(f'{started_at}: Запуск скрипта проверки наличия новых протоколов')

    try:
        new_data, for_find_dif, now_db_last_protocols = udf.check_new_protocols(credential)
    except Exception as e:
        print(f"❌ Ошибка в check_new_protocols: {e}")

        error_message = (
            f"*__🔴 record\\_latest\\_protocol__*\n\n"
            f"*Время запуска:* {escape_markdown(started_at.strftime('%Y-%m-%d %H:%M:%S'))}\n"
            f"*Ошибка:* {escape_markdown(str(e))}"
        )
        send_telegram_notification(error_message)
        return

    if new_data is None:
        print("ℹ️ check_new_protocols вернул None (нет данных для проверки).")

        message = (
            f"*__⚪ record\\_latest\\_protocol__*\n\n"
            f"*Время запуска:* {escape_markdown(started_at.strftime('%Y-%m-%d %H:%M:%S'))}\n"
            f"Нет данных для проверки\\."
        )
        send_telegram_notification(message)
        return

    different_list_of_protocols = udf.find_dif_list_protocol(for_find_dif, now_db_last_protocols)

    updated_stats = {
        "updated": 0,
        "no_changes": 0,
        "errors": 0,
        "updated_protocols": []
    }

    if not different_list_of_protocols.empty:
        updated_stats = func_update_protocols(different_list_of_protocols)

    new_protocols = []
    if not new_data.empty:
        print(f'Есть {len(new_data)} протоколов для записи в БД')
        data_protocols, data_protocol_vol = udf.get_list_protocol(new_data)
        udf.add_new_protocols(credential, new_data, data_protocols, data_protocol_vol)

        new_protocols = [
            f"{row['name_point']} — {row['date_event'].strftime('%Y-%m-%d')}"
            for _, row in new_data.iterrows()
        ]

    print('_' * 20)

    total_new = len(new_data)
    total_updated = updated_stats["updated"]
    total_errors = updated_stats["errors"]

    if total_errors > 0:
        status_emoji = "🟠"
    elif total_new > 0 or total_updated > 0:
        status_emoji = "🟢"
    else:
        status_emoji = "⚪"

    new_list_text = ""
    if new_protocols:
        new_list_text = "\n\n*Новые протоколы:*\n"
        for item in new_protocols:
            new_list_text += f"• {escape_markdown(item)}\n"

    updated_list_text = ""
    if updated_stats["updated_protocols"]:
        updated_list_text = "\n\n*Обновлённые протоколы:*\n"
        for item in updated_stats["updated_protocols"]:
            updated_list_text += f"• {escape_markdown(item)}\n"

    if total_new == 0 and total_updated == 0:
        message = (
            f"*__{status_emoji} record\\_latest\\_protocol__*\n\n"
            f"*Время запуска:* {escape_markdown(started_at.strftime('%Y-%m-%d %H:%M:%S'))}\n"
            f"*Новых протоколов:* 0\n"
            f"*Обновлено существующих:* 0\n"
            f"*Ошибок:* {total_errors}\n\n"
            f"Новых или изменённых протоколов не найдено\\."
        )
    else:
        message = (
            f"*__{status_emoji} record\\_latest\\_protocol__*\n\n"
            f"*Время запуска:* {escape_markdown(started_at.strftime('%Y-%m-%d %H:%M:%S'))}\n"
            f"*Новых протоколов:* {total_new}\n"
            f"*Обновлено существующих:* {total_updated}\n"
            f"*Без изменений:* {updated_stats['no_changes']}\n"
            f"*Ошибок:* {total_errors}"
            f"{new_list_text}"
            f"{updated_list_text}"
        )

    send_telegram_notification(message)


if __name__ == "__main__":
    record_latest_protocol()