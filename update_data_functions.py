import DB_handler as db
import pandas as pd
import link_handler
import parse_last_running as plr

def check_new_protocols(credential):
    '''Получаем данные протоколов, которые можно внести в БД'''
    #Получаем данные со списком протоколов
    engine = db.db_connect(credential)
    df = db.get_table(engine, 'list_all_events', 'index_event, name_point, date_event, link_event, is_test')
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