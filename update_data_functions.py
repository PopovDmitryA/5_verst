import DB_handler as db
import pandas as pd
import link_handler
import parse_last_running as plr
from tqdm import tqdm
import parse_protocol as pp

def check_new_protocols(engine):
    '''Получаем данные протоколов, которые можно внести в БД'''
    #Получаем данные со списком протоколов
    #engine = db.db_connect(credential)
    df = db.get_table(engine, 'list_all_events_copy', 'index_event, name_point, date_event, link_event, is_test')
    df['link_point'] = df['link_event'].apply(link_handler.main_link_event)
    df = df.drop(columns=['link_event'])
    DB_data = df[["index_event", "name_point", "date_event", "link_point", "is_test"]]

    #Парсим последние протоколы
    last_event = plr.transform_df_last_event(plr.last_event_parse())

    #Формируем таблицу, идентичную по структуре таблице из БД для сравнения
    compare_event = last_event[['index_event', 'name_point', 'date_event', 'link_point', 'is_test']].copy()

    # Объединяем датафреймы с помощью merge и добавляем индикатор совпадений
    merged_df = pd.merge(compare_event, DB_data, how='left', indicator=True)

    # Отбираем строки, которых нет в df2
    missing_rows = merged_df[merged_df['_merge'] == 'left_only']

    # Убираем лишний столбец с индикатором
    new_data = missing_rows.drop('_merge', axis=1)

    #По индексу фильтруем данные из спаршенной таблицы, чтобы подготовить данные для внесения в БД
    finish_df = last_event.loc[new_data.index,
                         ['index_event',
                          'name_point',
                          'date_event',
                          'link_event',
                          'is_test',
                          'count_runners',
                          'count_vol',
                          'mean_time',
                          'best_time_woman',
                          'best_time_man']]

    return finish_df

def add_new_protocols(credential):
    engine = db.db_connect(credential)
    new_data = check_new_protocols(engine)
    if len(new_data) > 0:
        engine = db.db_connect(credential)
        db.append_df(engine, 'list_all_events_copy', new_data)
        #db.info_table_update(engine, table_name, upd_time)
        data_protocols, data_protocol_vol = pd.DataFrame(), pd.DataFrame()

        count = len(new_data)
        for index, row in tqdm(new_data.iterrows(), total=count):
            link = row['link_event']
            final_df_run, final_df_vol = pp.main_parse(link)
            data_protocols = pd.concat([data_protocols, final_df_run], ignore_index=True)
            data_protocol_vol = pd.concat([data_protocol_vol, final_df_vol], ignore_index=True)

        engine = db.db_connect(credential)
        db.append_df(engine, 'details_protocol_copy', data_protocols)
        engine = db.db_connect(credential)
        db.append_df(engine, 'details_vol_copy', data_protocol_vol)