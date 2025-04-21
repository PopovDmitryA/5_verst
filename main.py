#import parse_protocol
# import parse_table_protocols_in_park as ptpp
# import parse_last_running as plr
# import DB_handler as db
from datetime import datetime
import configparser
import update_data_functions as udf

config = configparser.ConfigParser()
config.read('/Users/dmitry/PycharmProjects/5_verst/5_verst.ini')

db_host = config['five_verst_stats']['host']
db_user = config['five_verst_stats']['username']
db_pass = config['five_verst_stats']['password']
db_name = config['five_verst_stats']['dbname']

credential = f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}'

#def main():
    # link = 'https://5verst.ru/aleksandrino/results/22.04.2023/'
    # final_df_run, final_df_vol = parse_protocol.main_parse(link)
    # print(final_df_run, final_df_vol)
    # link = 'https://5verst.ru/purnavolok/results/all/'
    # all_protocol = ptpp.transform_df_list_protocol(ptpp.list_protocols_in_park(link))
    # print(all_protocol)
    # print(plr.transform_df_last_event(plr.last_event_parse()))
    #
    # engine = db.db_connect(credential)
    # print(db.get_table(engine, 'list_all_events'))
    #print(udf.check_new_protocols(credential))

def update_protocols():
    list_site_protocols, now_table = udf.get_list_all_protocol(credential)
    list_different = udf.find_dif(list_site_protocols, now_table)
    #Выше мы составили список парков, которые нужно перевыгрузить, далее нужно выгрузить по ним протоколы, удалить данные и записать данные

def record_latest_protocol():
    print(f'{datetime.now()}: Запуск скрипта проверки наличия новых протоколов')
    new_data = udf.check_new_protocols(credential)
    if len(new_data) == 0:
        return
    print(f'Есть {len(new_data)} протоколов для записи в БД')

    data_protocols, data_protocol_vol = udf.get_list_protocol(new_data)
    udf.add_new_protocols(credential, new_data, data_protocols, data_protocol_vol)
    print('_' * 20)

if __name__ == '__main__':
    #main()
    record_latest_protocol()
    #update_protocols()
