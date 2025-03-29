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

def main():
    #link = 'https://5verst.ru/aleksandrino/results/22.04.2023/'
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
    print(f'{datetime.now()}: Запуск скрипта проверки наличия новых протоколов')
    udf.add_new_protocols(credential)
    print('_' * 20)
if __name__ == '__main__':
    main()