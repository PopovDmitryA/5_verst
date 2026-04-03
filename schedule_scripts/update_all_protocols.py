from update_data_functions import get_list_all_protocol, find_dif_list_protocol
from .update_data_main import func_update_protocols, credential
from telegram_notifier import send_telegram_notification, escape_markdown
from datetime import datetime

def update_protocols():
    """Проверяем изменения в саммари-протоколах каждого парка, сверяем с БД и обновляем данные"""
    started_at = datetime.now()
    print(f'{started_at}: Запуск скрипта полной проверки протоколов')

    try:
        list_site_protocols, now_table = get_list_all_protocol(credential)
        different_list_of_protocols = find_dif_list_protocol(list_site_protocols, now_table)

        diff_count = len(different_list_of_protocols)

        if not different_list_of_protocols.empty:
            stats = func_update_protocols(different_list_of_protocols)

            if stats["errors"] > 0:
                status_emoji = "🟠"
            elif stats["updated"] > 0:
                status_emoji = "🟢"
            else:
                status_emoji = "⚪"

            updated_list_text = ""
            if stats["updated_protocols"]:
                updated_list_text = "\n\n*Обновлённые протоколы:*\n"
                for item in stats["updated_protocols"]:
                    updated_list_text += f"• {escape_markdown(item)}\n"

            if stats["updated"] == 0:
                message = (
                    f"*__{status_emoji} update\\_all\\_protocols__*\n\n"
                    f"*Время запуска:* {escape_markdown(started_at.strftime('%Y-%m-%d %H:%M:%S'))}\n"
                    f"*Проверено summary\\-протоколов:* {len(list_site_protocols)}\n"
                    f"*Найдено протоколов с отличиями:* {diff_count}\n"
                    f"*Обновлено:* {stats['updated']}\n"
                    f"*Без изменений:* {stats['no_changes']}\n"
                    f"*Ошибок:* {stats['errors']}\n\n"
                    f"Изменений в протоколах не найдено\\."
                )
            else:
                message = (
                    f"*__{status_emoji} update\\_all\\_protocols__*\n\n"
                    f"*Время запуска:* {escape_markdown(started_at.strftime('%Y-%m-%d %H:%M:%S'))}\n"
                    f"*Проверено summary\\-протоколов:* {len(list_site_protocols)}\n"
                    f"*Найдено протоколов с отличиями:* {diff_count}\n"
                    f"*Обновлено:* {stats['updated']}\n"
                    f"*Без изменений:* {stats['no_changes']}\n"
                    f"*Ошибок:* {stats['errors']}"
                    f"{updated_list_text}"
                )
        else:
            print('Протоколов для обновления не найдено')
            message = (
                f"*__⚪ update\\_all\\_protocols__*\n\n"
                f"*Время запуска:* {escape_markdown(started_at.strftime('%Y-%m-%d %H:%M:%S'))}\n"
                f"*Проверено summary\\-протоколов:* {len(list_site_protocols)}\n"
                f"Отличий не найдено\\."
            )

        send_telegram_notification(message)
        print('_' * 20)

    except Exception as e:
        error_message = (
            f"*__🔴 update\\_all\\_protocols__*\n\n"
            f"*Время запуска:* {escape_markdown(started_at.strftime('%Y-%m-%d %H:%M:%S'))}\n"
            f"*Ошибка:* {escape_markdown(str(e))}"
        )
        send_telegram_notification(error_message)
        raise

if __name__ == "__main__":
    update_protocols()