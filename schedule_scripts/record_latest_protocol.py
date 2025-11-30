import update_data_functions as udf
from .update_data_main import func_update_protocols, credential
from datetime import datetime

def record_latest_protocol():
    """Записываем новые пробежки и обновляем те, которые подверглись изменениям
    в саммари со страницы последних забегов.
    """
    print(f'{datetime.now()}: Запуск скрипта проверки наличия новых протоколов')

    try:
        new_data, for_find_dif, now_db_last_protocols = udf.check_new_protocols(credential)
    except Exception as e:
        # На всякий случай, чтобы сетевые/парсинговые ошибки не валили крон
        print(f"❌ Ошибка в check_new_protocols: {e}")
        return

    # Если last_event пустой — check_new_protocols вернёт пустые df, просто ничего не делаем
    if new_data is None:
        print("ℹ️ check_new_protocols вернул None (нет данных для проверки).")
        return

    different_list_of_protocols = udf.find_dif_list_protocol(for_find_dif, now_db_last_protocols)

    if not different_list_of_protocols.empty:
        func_update_protocols(different_list_of_protocols)

    if not new_data.empty:
        print(f'Есть {len(new_data)} протоколов для записи в БД')
        data_protocols, data_protocol_vol = udf.get_list_protocol(new_data)
        udf.add_new_protocols(credential, new_data, data_protocols, data_protocol_vol)

    print('_' * 20)


if __name__ == "__main__":
    record_latest_protocol()
