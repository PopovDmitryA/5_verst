import DB_handler as db
import pandas as pd
import link_handler
import parse_last_running as plr
from tqdm import tqdm
import parse_protocol as pp
import parse_table_protocols_in_park as ptpp

def check_new_protocols(credential):
    '''Получаем данные протоколов, которые можно внести в БД'''
    #Получаем данные со списком протоколов
    engine = db.db_connect(credential)
    df = db.get_table(engine, 'list_all_events', 'index_event, name_point, date_event, link_event, is_test')
    df['link_point'] = df['link_event'].apply(link_handler.main_link_event)
    df = df.drop(columns=['link_event'])
    db_data = df[["index_event", "name_point", "date_event", "link_point", "is_test"]]

    #Парсим последние протоколы
    last_event = plr.transform_df_last_event(plr.last_event_parse())

    #Формируем таблицу, идентичную по структуре таблице из БД для сравнения
    compare_event = last_event[['index_event', 'name_point', 'date_event', 'link_point', 'is_test']].copy()

    # Объединяем датафреймы с помощью merge и добавляем индикатор совпадений
    merged_df = pd.merge(compare_event, db_data, how='left', indicator=True)

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

def get_list_protocol(new_data):
    data_protocols, data_protocol_vol = pd.DataFrame(), pd.DataFrame()
    for _, row in new_data.iterrows():
        link = row['link_event']
        final_df_run, final_df_vol = pp.main_parse(link)
        data_protocols = pd.concat([data_protocols, final_df_run], ignore_index=True)
        data_protocol_vol = pd.concat([data_protocol_vol, final_df_vol], ignore_index=True)
        print(f'\t{row["date_event"]} - {row["name_point"]}: {row["count_runners"]} участников, {row["count_vol"]} волонтеров')
    return data_protocols, data_protocol_vol

def add_new_protocols(credential, new_data, data_protocols, data_protocol_vol):
    '''Проверяем наличие протоколов, которые можно записать в БД, записываем + парсим детали протокола и записываем их в базу'''
    #engine = db.db_connect(credential)
    # new_data = check_new_protocols(credential)
    # if len(new_data) == 0:
    #     return
    # print(f'Есть {len(new_data)} протоколов для записи в БД')
    engine = db.db_connect(credential)
    db.append_df(engine, 'list_all_events', new_data)
    #data_protocols, data_protocol_vol = pd.DataFrame(), pd.DataFrame()

    # for _, row in new_data.iterrows():
    #     link = row['link_event']
    #     final_df_run, final_df_vol = pp.main_parse(link)
    #     data_protocols = pd.concat([data_protocols, final_df_run], ignore_index=True)
    #     data_protocol_vol = pd.concat([data_protocol_vol, final_df_vol], ignore_index=True)
    #     print(f'\t{row["date_event"]} - {row["name_point"]}: {row["count_runners"]} участников, {row["count_vol"]} волонтеров')

    #engine = db.db_connect(credential)
    db.append_df(engine, 'details_protocol', data_protocols)
    db.update_view(engine, 'new_turists')

    #engine = db.db_connect(credential)
    db.append_df(engine, 'details_vol', data_protocol_vol)
    db.update_view(engine, 'new_turists_vol')

    return print(f'В БД Записано {len(new_data)} протоколов, {len(data_protocols)} строчек бегунов, {len(data_protocol_vol)} строчек волонтеров')

def get_list_all_protocol(credential):
    '''Функция собирает данные со страниц всех протоколов каждого парка + получает из БД аналогичную таблицу, чтобы потом сравнить'''
    engine = db.db_connect(credential)
    # Берем ссылки на локации из БД и таблицу, с которой в дальнейшем будем сравнивать собранный ниже df
    result = db.get_table(engine, 'general_link_all_location')
    table = db.get_table(engine, 'list_all_events')
    empty_df = pd.DataFrame()

    count = len(result)
    # Собираем в единый df все данные со списком протоколов каждой локации
    for _, row in tqdm(result.iterrows(), total=count):
        link = link_handler.link_all_result_event(row['link_point'])
        all_point_protocol = ptpp.transform_df_list_protocol(ptpp.list_protocols_in_park(link))
        empty_df = pd.concat([empty_df, all_point_protocol], ignore_index=True)

    return empty_df, table

def find_dif(list_site_protocols, now_table):
    '''Функция формирует сравнивает два dataframe и выводит значения из list_site_protocols, которые отличны в таблице now_table, возвращает готовый для update df'''
    diff_right = list_site_protocols.merge(now_table, how='left', indicator=True)
    diff_right = diff_right[diff_right['_merge'] == 'left_only'].drop('_merge', axis=1)

    print(diff_right)
    diff_right.to_excel('/Users/dmitry/PycharmProjects/5_verst/test.xlsx', index=False)
    return diff_right
