from update_data_functions import get_list_all_protocol, find_dif_list_protocol
from .update_data_main import func_update_protocols, credential
from telegram_notifier import send_telegram_notification
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
            message = (
                f"🔄 update_all_protocols\n"
                f"Время запуска: {started_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"Проверено summary-протоколов: {len(list_site_protocols)}\n"
                f"Найдено протоколов с отличиями: {diff_count}\n"
                f"Обновлено: {stats['updated']}\n"
                f"Без изменений: {stats['no_changes']}\n"
                f"Ошибок: {stats['errors']}"
            )
        else:
            print('Протоколов для обновления не найдено')
            message = (
                f"✅ update_all_protocols\n"
                f"Время запуска: {started_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"Проверено summary-протоколов: {len(list_site_protocols)}\n"
                f"Отличий не найдено"
            )

        send_telegram_notification(message)
        print('_' * 20)

    except Exception as e:
        error_message = (
            f"❌ update_all_protocols\n"
            f"Время запуска: {started_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Ошибка: {e}"
        )
        send_telegram_notification(error_message)
        raise

if __name__ == "__main__":
    update_protocols()