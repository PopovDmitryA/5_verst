from update_data_functions import get_list_all_protocol, find_dif_list_protocol
from .update_data_main import func_update_protocols, credential
from datetime import datetime

def update_protocols():
    '''Проверяем изменения в саммари-протоколах каждого парка, сверяем с БД и обновляем данные'''
    print(f'{datetime.now()}: Запуск скрипта полной проверки протоколов')
    list_site_protocols, now_table = get_list_all_protocol(credential)
    different_list_of_protocols = find_dif_list_protocol(list_site_protocols, now_table)

    if not different_list_of_protocols.empty:
        func_update_protocols(different_list_of_protocols)
    else:
        print('Протоколов для обновления не найдено')
    print('_' * 20)

if __name__ == "__main__":
    update_protocols()
