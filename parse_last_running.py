import pandas as pd
import requests
import link_handler
from bs4 import BeautifulSoup


def last_event_parse():
    '''Парсим страницу с последними пробежками всех парков'''
    page = 'https://5verst.ru/results/latest/'
    t = requests.get(page)
    soup = BeautifulSoup(t.text, 'html.parser')

    table_last_event = soup.find('table', {'class': 'sortable results-table min-w-full leading-normal'})
    rows = table_last_event.find_all('tr')
    data = []
    for index, row in enumerate(rows):
        cols = [col.text.strip() for col in row.find_all(['td', 'th'])]
        find_link = row.find('a')
        if find_link is not None:
            link_runner = find_link.get('href')
            cols += [link_runner]
        data.append(cols)
    data[0] += ['link_event']
    df = pd.DataFrame(data[1:], columns=data[0])

    return df


def transform_df_last_event(df):
    '''Преобразуем таблицу в формат для БД'''
    df_copy = df.copy()
    df_copy['name_point'] = df_copy['Старт #'].apply(lambda x: x.split('#')[0].strip())
    df_copy['index_event'] = df_copy['Старт #'].apply(lambda x: x.split('#', 1)[-1].strip() if '#' in x else '0')
    #df_copy['index_event'] = pd.to_numeric(df_copy['index_event'], errors='coerce').astype('Int64')
    df_copy['is_test'] = df_copy['Старт #'].apply(lambda x: False if '#' in x else True)

    df_copy['link_point'] = df_copy['link_event'].apply(link_handler.main_link_event)
    df_copy['date_event'] = pd.to_datetime(df_copy['Дата'], format='%d.%m.%Y')

    df_copy['link_event'] = df_copy.apply(lambda row: link_handler.link_protocol_from_date(row['link_point'], row['date_event']),
                                          axis=1)
    df_copy = df_copy.drop(columns=['Старт #', 'Дата'])
    # Переименовываем столбцы
    df_copy = df_copy.rename(columns={"Финишёров": "count_runners",
                                      "Волонтёров": "count_vol",
                                      "Среднее время": "mean_time",
                                      'Лучшее "Ж"': "best_time_woman",
                                      'Лучшее "М"': "best_time_man"})

    # Меняем порядок столбцов
    df_copy = df_copy[["index_event",
                       "name_point",
                       "date_event",
                       'link_event',
                       "link_point",
                       "is_test",
                       'count_runners',
                       'count_vol',
                       'mean_time',
                       'best_time_woman',
                       'best_time_man']]

    for col in ['mean_time', 'best_time_woman', 'best_time_man']:
        df_copy[col] = df_copy[col].replace('', pd.NA).fillna('00:00:00')
        df_copy[col] = pd.to_datetime(df_copy[col], format='%H:%M:%S', errors='coerce').dt.time

    for col in ['index_event', 'count_runners', 'count_vol']:
        df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').astype('Int64')

    return df_copy