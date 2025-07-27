from update_data_functions import get_link_protocols_for_update, get_list_protocol, get_now_protocols, find_dif_protocol
from update_protocols import update_data_protocols
from .update_data_main import credential
from datetime import datetime

def find_dif_details_protocol(count_last_protocol=3, name_point=[]):
    """
    Функция сверяет детали по последним x протоколам по каждому парку.

    :param count_last_protocol: количество последних стартов, которое мы проверяем у каждого парка
    :param start_point: порядковый номер локации, с которого делаем выгрузку
    :param finish_point: порядковый номер локации, по которую делаем выгрузку
    :param name_point: наименование локации, если хотим выгрузить только какую-то конкретную
    """
    if name_point:
        list_protocols = get_link_protocols_for_update(credential, count_last_protocol, name_point)
    else:
        list_protocols = get_link_protocols_for_update(credential, count_last_protocol)

    print(f'{datetime.now()}: Обновление {len(list_protocols)} протоколов')

    actual_protocols, actual_protocol_vol = get_list_protocol(list_protocols) # Парсим внутренности протоколов для сравнения
    not_actual_protocols, not_actual_protocol_vol = get_now_protocols(credential, list_protocols) # Получаем из БД данные этих же протоколов для сравнения

    print(f'''Выгрузили с сайта {len(actual_protocols)} строчек с бегунами и {len(actual_protocol_vol)} строк с волонтёрами
Получили из БД по аналогичным протоколам {len(not_actual_protocols)} строк с бегунами и {len(not_actual_protocol_vol)} строк с волонтёрами
Приступаем к сравнению и обновлению данных''')

    for_removal_runner, to_add_runner = find_dif_protocol(actual_protocols, not_actual_protocols)
    for_removal_vol, to_add_vol = find_dif_protocol(actual_protocol_vol, not_actual_protocol_vol)

    if any(not df.empty for df in [for_removal_runner, for_removal_vol, to_add_runner, to_add_vol]):
        update_data_protocols(credential, for_removal_runner, for_removal_vol, to_add_runner, to_add_vol)
        print(f'''
Обновили данные в БД
Из таблицы бегунов удалили {len(for_removal_runner)} старых записей, добавили {len(to_add_runner)} записей
Из таблицы волонтеров удалили {len(for_removal_vol)} старых записей, добавили {len(to_add_vol)} записей
        ''')
    else:
        print('Нет изменений')

if __name__ == "__main__":
    import sys
    import ast

    try:
        count = int(sys.argv[1]) if len(sys.argv) > 1 else None
        parks = ast.literal_eval(sys.argv[2]) if len(sys.argv) > 2 else None

        if count is not None and parks is not None:
            find_dif_details_protocol(count, parks)
        elif count is not None:
            find_dif_details_protocol(count)
        elif parks is not None:
            find_dif_details_protocol(0, parks)  # предполагаем, что если count не указан, берём 0
        else:
            find_dif_details_protocol()

    except Exception as e:
        print(f"""
Ошибка запуска: {e}
Примеры:
  python update_recent_by_count.py 5
  python update_recent_by_count.py 5 "['Сосновка', 'Коломяги']"
  python update_recent_by_count.py
""")
