import requests, re
from bs4 import BeautifulSoup
import pandas as pd

def list_protocols_in_park(link):
    '''Парсим страницу с последними пробежками по парку'''
    t = requests.get(link)
    soup = BeautifulSoup(t.text, 'html.parser')

    name_point_parse = soup.find_all('li', class_='menu-item menu-item-type-custom menu-item-object-custom')
    name_point = name_point_parse[2].get_text()

    table_protocols = soup.find('table', {'class': 'sortable results-table min-w-full leading-normal'})
    rows = table_protocols.find_all('tr')
    data = []
    for index, row in enumerate(rows):
        cols = [col.text.strip() for col in row.find_all(['td', 'th'])]
        find_link = row.find('a')
        if find_link is not None:
            name_runner, link_runner = find_link.text, find_link.get('href')
            cols += [name_runner, link_runner]
        data.append(cols)
    data[0] += ['date_event', 'link_event']
    df = pd.DataFrame(data[1:], columns=data[0])
    df['name_point'] = name_point

    return df

def transform_df_list_protocol(list_protocol):
    '''Преобразуем таблицу в формат для БД'''
    df_copy = list_protocol.copy()
    df_copy = df_copy.drop(columns=['Дата'])

    # Переименовываем столбцы
    df_copy = df_copy.rename(columns={"##": "index_event",
                                      "Финишёров": "count_runners",
                                      "Волонтёров": "count_vol",
                                      "Среднее время": "mean_time",
                                      'Лучшее "Ж"': "best_time_woman",
                                      'Лучшее "М"': "best_time_man"})

    df_copy['is_test'] = df_copy['index_event'].apply(lambda x: pd.isnull(x) or x == '')
    # Меняем порядок столбцов
    df_copy = df_copy[["index_event",
                       "name_point",
                       "date_event",
                       "link_event",
                       "is_test",
                       "count_runners",
                       'count_vol',
                       'mean_time',
                       'best_time_woman',
                       'best_time_man']]  # Новый порядок
    df_copy['index_event'] = df_copy['index_event'].replace('', 0).astype(int)
    df_copy['date_event'] = pd.to_datetime(df_copy['date_event'], format='%d.%m.%Y', errors='coerce')
    for col in ['mean_time', 'best_time_woman', 'best_time_man']:
        df_copy[col] = pd.to_datetime(df_copy[col], format='%H:%M:%S', errors='coerce')
    for col in ['index_event', 'count_runners', 'count_vol']:
        df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').astype('Int64')

    return df_copy