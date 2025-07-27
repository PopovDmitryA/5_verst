import configparser
from update_data_functions import get_list_protocol, get_now_protocols, find_dif_protocol, create_list_for_compare
from update_protocols import update_data_protocols
import pandas as pd

config = configparser.ConfigParser()
config.read('/Users/dmitry/PycharmProjects/5_verst/5_verst.ini')

db_host = config['five_verst_stats']['host']
db_user = config['five_verst_stats']['username']
db_pass = config['five_verst_stats']['password']
db_name = config['five_verst_stats']['dbname']

credential = f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}'

def func_update_protocols(different_list_of_protocols):
    print(f'Нашли {len(different_list_of_protocols)} протоколов с отличиями')
    actual_protocols, actual_protocol_vol = get_list_protocol(different_list_of_protocols)
    not_actual_protocols, not_actual_protocol_vol = get_now_protocols(credential, different_list_of_protocols)

    for_removal_runner, to_add_runner = find_dif_protocol(actual_protocols, not_actual_protocols)

    if not_actual_protocol_vol.empty:
        to_add_vol = actual_protocol_vol
        for_removal_vol = pd.DataFrame()
    else:
        for_removal_vol, to_add_vol = find_dif_protocol(actual_protocol_vol, not_actual_protocol_vol)

    update_data_protocols(credential, for_removal_runner, for_removal_vol, to_add_runner, to_add_vol, different_list_of_protocols)

    print(f'''
    Обновили данные по {len(different_list_of_protocols)} протоколам
    Удалено {len(for_removal_runner)} бегунов, добавлено {len(to_add_runner)}
    Удалено {len(for_removal_vol)} волонтеров, добавлено {len(to_add_vol)}
    ''')

def list_point_update():
    return create_list_for_compare(credential)
