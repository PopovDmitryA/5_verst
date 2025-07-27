import DB_handler as db
import link_handler
import parse_last_running as plr
import parse_protocol as pp
import parse_table_protocols_in_park as ptpp

import pandas as pd
import time
from tqdm import tqdm

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
    # Переменная с порядком столбцов
    column_order = [
        'index_event',
        'name_point',
        'date_event',
        'link_event',
        'is_test',
        'count_runners',
        'count_vol',
        'mean_time',
        'best_time_woman',
        'best_time_man'
    ]
    finish_df = last_event.loc[new_data.index, column_order]

    # сохраняем исходный df с сайта и получаем аналогичный из БД, чтобы их сравнить
    for_find_dif = last_event[column_order]
    now_db_last_protocols = pd.DataFrame()
    for _, row in for_find_dif.iterrows():
        сondition = [{'name_point': row['name_point']}, {'date_event': row['date_event']}]  # Формируем условие
        temp_now_protocol = db.get_inf_with_condition(engine, 'list_all_events', сondition)
        now_db_last_protocols = pd.concat([now_db_last_protocols, temp_now_protocol], ignore_index=True)
    now_db_last_protocols = now_db_last_protocols.drop(columns=['updated_at'])

    return finish_df, for_find_dif, now_db_last_protocols

def get_list_protocol(new_data):
    '''В цикле проходимся по каждой строчке df, со ссылками на протоколы, парсим сами протоколы и собираем это в единую таблицу'''
    data_protocols, data_protocol_vol = pd.DataFrame(), pd.DataFrame()
    counter = 0
    for _, row in new_data.iterrows():
        if counter >= 50:
            print('Сплю 30 сек')
            time.sleep(30)
            counter = 0
        link = row['link_event']
        final_df_run, final_df_vol = pp.main_parse(link)
        data_protocols = pd.concat([data_protocols, final_df_run], ignore_index=True)
        data_protocol_vol = pd.concat([data_protocol_vol, final_df_vol], ignore_index=True)
        print(f'\t{row["date_event"]} - {row["name_point"]}: {row["count_runners"]} участников, {row["count_vol"]} волонтеров')
        counter += 1
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
    request = 'SELECT general_link_all_location.* FROM general_link_all_location join general_location gl using (name_point) where is_pause = false'
    result = db.execute_request(engine, request)
    #result = result.iloc[121:] #Если нужно обновить данные по частям

    #Часть ниже реализована для запуска скрипта не на весь пул протоколов, а на часть
    where_conditions = []
    for _, row in result.iterrows():
        value = row['name_point']
        where_conditions.append(f"name_point ='{value}'")
    where_clause = " OR ".join(where_conditions)

    request_now = f'SELECT list_all_events.* FROM list_all_events join general_location gl using (name_point) where is_pause = false and {where_clause}'

    table = db.execute_request(engine, request_now)
    table = table.drop(columns=['updated_at'])
    empty_df = pd.DataFrame()

    count = len(result)
    # Собираем в единый df все данные со списком протоколов каждой локации
    for _, row in tqdm(result.iterrows(), total=count):
        link = link_handler.link_all_result_event(row['link_point'])
        all_point_protocol = ptpp.transform_df_list_protocol(ptpp.list_protocols_in_park(link))
        empty_df = pd.concat([empty_df, all_point_protocol], ignore_index=True)
    #print(len(empty_df), len(table))
    print('Спарсили списки всех протоколов для сравнения')
    return empty_df, table

def find_dif_list_protocol(list_site_protocols, now_table):
    '''Функция сравнивает два dataframe и выводит значения из list_site_protocols, которые отличны в таблице now_table, возвращает готовый для update df.
    Предварительно функция удаляет протоколы, которые еще не обрабатывались основным скриптом и не вносилась информация в БД о них'''

    # Создаем временный DataFrame с нужными колонками для сравнения
    temp_list_site_protocols = list_site_protocols[['name_point', 'date_event']]
    temp_now_table = now_table[['name_point', 'date_event']]

    # Объединяем temp_empty_df и temp_table по нужным столбцам
    diff_right_new = temp_list_site_protocols.merge(temp_now_table, on=['name_point', 'date_event'], how='left', indicator=True)

    # Фильтруем записи, существующие только слева (левое слияние)
    diff_right_new = diff_right_new[diff_right_new['_merge'] == 'left_only']

    if len(diff_right_new) != 0:
        print('Есть протокол, не записанный в БД')
        print(diff_right_new)
        # Получаем индексы записей, уникальные для left_dataframe
        unique_indices = diff_right_new.index

        # Удаляем эти записи из original dataframe
        list_site_protocols = list_site_protocols.drop(unique_indices).reset_index(drop=True)

    #Производим фильтрацию по всем столбцам, чтобы отобрать строчки, которые требуют обновления
    diff_right = list_site_protocols.merge(now_table, how='left', indicator=True)
    diff_right = diff_right[diff_right['_merge'] == 'left_only'].drop('_merge', axis=1)
    diff_right = diff_right.sort_values(by = ['date_event', 'name_point'], ascending=True)

    return diff_right

def get_now_protocols(credential, different_list_of_protocols):
    '''Функция собирает 2 df с текущей информацией из протоколов, которая находится в БД'''
    engine = db.db_connect(credential)

    result_run, result_vol = pd.DataFrame(), pd.DataFrame()
    for _, row in different_list_of_protocols.iterrows():
        сondition = [{'name_point': row['name_point']}, {'date_event': row['date_event']}] #Формируем условие
        temp_result_run = db.get_inf_with_condition(engine, 'details_protocol', сondition)
        temp_result_vol = db.get_inf_with_condition(engine, 'details_vol', сondition)
        result_run = pd.concat([result_run, temp_result_run], ignore_index=True)
        # Проверяем наличие данных в temp_result_vol перед конкатенацией
        if not temp_result_vol.empty and not temp_result_vol.isna().all(axis=None):
            result_vol = pd.concat([result_vol, temp_result_vol], ignore_index=True)
        else:
            # Можно оставить сообщение для отладки или вообще ничего не делать
            pass

    result_run = result_run.drop(columns=['updated_at'])
    if len(result_vol) != 0:
        result_vol = result_vol.drop(columns=['updated_at'])

    return result_run, result_vol

def find_dif_protocol(actual_df, not_actual_df):
    '''Функция ищет отличия в двух датафреймах и возвращает df для добавления в БД и df со строчками для удаления из БД данных'''
    diff_left = not_actual_df.merge(actual_df, how='left', indicator=True)
    diff_right = not_actual_df.merge(actual_df, how='right', indicator=True)
    for_delete = diff_left[diff_left['_merge'] != 'both']
    to_add = diff_right[diff_right['_merge'] != 'both']

    for_delete = for_delete.drop(columns=['_merge'])
    to_add = to_add.drop(columns=['_merge'])

    return for_delete, to_add


def get_link_protocols_for_update(credential, count_last_protocol, name_point=[]):
    '''Функция возвращает список протоколов для сверки'''

    if len(name_point) != 0:
        result_list = [{'name_point': point} for point in name_point]
        condition = db.create_condition(result_list, 'OR')

        if count_last_protocol > 0:
            date_filter = f'''
                date_event IN (
                    SELECT DISTINCT date_event
                    FROM list_all_events
                    ORDER BY date_event DESC
                    LIMIT {count_last_protocol}
                )
            '''
        else:
            date_filter = 'TRUE'  # Без ограничения по датам

        request = f'''
SELECT *
FROM list_all_events
WHERE {date_filter}
  AND ({condition});
        '''
    else:
        if count_last_protocol > 0:
            request = f'''
SELECT *
FROM list_all_events
WHERE date_event IN (
    SELECT DISTINCT date_event
    FROM list_all_events
    ORDER BY date_event DESC
    LIMIT {count_last_protocol}
);
'''
        else:
            request = '''
SELECT *
FROM list_all_events;
'''

    engine = db.db_connect(credential)
    result = db.execute_request(engine, request)

    return result

def create_list_for_compare(credential):
    engine = db.db_connect(credential)
    request = 'SELECT name_point FROM general_location where is_pause = false'
    result = db.execute_request(engine, request)
    values_list = result.iloc[:, 0].tolist()
    return values_list