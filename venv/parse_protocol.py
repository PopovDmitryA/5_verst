import pandas as pd
import requests, re
from bs4 import BeautifulSoup
from datetime import datetime

def parse_runner(t):
    '''Парсинг бегунов со ссылками'''
    columns = ['name_runner', 'link_runner']
    df_run_link = pd.DataFrame(columns=columns)
    soup_run = BeautifulSoup(t.text, 'html.parser').find('div', class_='results-table__wrapper')
    for link_run in soup_run.find_all('a'):
        FIO_run = link_run.text
        link_runner = link_run.get('href')
        new_row = {'name_runner': FIO_run,
                   'link_runner': link_runner}
        df_run_link = pd.concat([df_run_link, pd.DataFrame([new_row])], ignore_index=True)
    return df_run_link

def parse_vol(t):
    '''Парсинг волонтеров со ссылками'''
    columns = ['name_runner', 'link_runner']
    df_vol_link = pd.DataFrame(columns=columns)
    soup_vol = BeautifulSoup(t.text, 'html.parser').find('div', class_='results-table__wrapper shadow rounded-lg')
    for link_vol in soup_vol.find_all('a'):
        FIO_vol = link_vol.text
        link_runner = link_vol.get('href')
        new_row = {'name_runner': FIO_vol,
                   'link_runner': link_runner}
        df_vol_link = pd.concat([df_vol_link, pd.DataFrame([new_row])], ignore_index=True)
        df_vol_link = df_vol_link.drop_duplicates()  # Удаляем дубликаты, т.к. эта таблица будет только справочником
    return df_vol_link

def identification_park_date(link, t):
    '''Определяем название парка и дату пробежки'''
    date_event = link.split('/')[5]
    soup = BeautifulSoup(t.text, 'html.parser').find('div', class_='page-header page-results-header')
    for link_run in soup.find_all('h1'):
        k = link_run.text
    name_point = k.split('Протокол 5 вёрст')[1].split('(')[0].strip()
    return date_event, name_point

def parse_protocol(link):
    '''Функция, возвращающая по ссылке на протокол 4 DF со страницы протоколов'''
    parse_site = pd.read_html(link)  # Парсим табличку
    t = requests.get(link)  # Парсим страницу

    date_event, name_point = identification_park_date(link, t)

    if len(parse_site) == 1:  # Если волонтерства не отгружены
        df_run = pd.DataFrame(parse_site[0])
        df_run_link = parse_runner(t)
        df_vol, df_vol_link = False, False
    else:
        df_run, df_vol = pd.DataFrame(parse_site[0]), pd.DataFrame(parse_site[1])
        df_run_link = parse_runner(t)
        df_vol_link = parse_vol(t)

    return df_run, df_vol, df_run_link, df_vol_link, date_event, name_point

def slice_before_parenthesis(value):
    '''Отделяем возрастную группу от мусора'''
    if isinstance(value, str):  # Проверка, является ли значение строкой
        # Используем регулярное выражение для удаления пробела перед "("
        return re.split(r'\s*\(', value)[0].strip()  # Убираем пробелы в начале и конце
    return value  # Возвращаем None или другое значение без изменений

def extract_user_id(link):
    '''Достаем id участника из ссылки'''
    try:
        return link.split('userstats/')[1]
    except IndexError:
        return None

def union_run(run, run_link, date_event, name_point):
    '''Объединяем два df, заменяем мусорный столбец с именами на нормальный'''
    for index, row in run_link.iterrows():
        indices = run.index[run['Участник'].str.contains(row['name_runner'])]
        if len(indices) == 1:
            run.at[indices[0], 'Участник'] = row['name_runner']
            run.at[indices[0], 'link_runner'] = row['link_runner']
            run.at[indices[0], 'user_id'] = extract_user_id(row['link_runner'])
            run.at[indices[0], 'date_event'] = date_event
            run.at[indices[0], 'name_point'] = name_point
        else:
            print('Два участника с одним ФИО', row['name_runner'])
    run['date_event'] = pd.to_datetime(run['date_event'], format='%d.%m.%Y')
    return run

def check_status_runner(new_df_run):
    '''Дополняем df столбцом о статусе участника'''
    for index, row in new_df_run.iterrows():
        if row['name_runner'] == 'НЕИЗВЕСТНЫЙ':
            new_df_run.at[index, 'status_runner'] = 'unknown_runner'
        elif row['user_id'] == None:
            new_df_run.at[index, 'status_runner'] = 'unregistered_runner'
        else:
            new_df_run.at[index, 'status_runner'] = 'active_runner'
    return new_df_run

def processing_run(df_run, df_run_link, date_event, name_point):
    '''Формируем финальный формат df пробежки для БД'''
    df_run_copy = df_run.copy()
    df_run_copy['age_category'] = df_run_copy['Возрастной рейтинг'].apply(slice_before_parenthesis)
    df_run_copy.drop(columns=['Возрастной рейтинг'], inplace=True)
    new_df_run = union_run(df_run_copy, df_run_link, date_event, name_point)

    new_df_run.rename(columns={
        '##': 'position',
        'Участник': 'name_runner',
        'Время': 'finish_time'
    }, inplace=True)

    new_df_run = check_status_runner(new_df_run)

    new_column_order = ['name_point',
                        'date_event',
                        'name_runner',
                        'link_runner',
                        'user_id',
                        'position',
                        'finish_time',
                        'age_category',
                        'status_runner']
    new_df_run = new_df_run.reindex(columns=new_column_order)

    return new_df_run

def processing_vol(df_vol, df_vol_link, date_event, name_point):
    '''Формируем финальный формат df волонтёров для БД'''
    df_vol_copy = df_vol.copy()
    new_df_vol = union_vol(df_vol_copy, df_vol_link, date_event, name_point)

    new_df_vol.rename(columns={
        'Волонтёр': 'name_runner',
        'Роль': 'vol_role'
    }, inplace=True)

    new_column_order = ['name_point',
                        'date_event',
                        'name_runner',
                        'link_runner',
                        'user_id',
                        'vol_role']
    new_df_vol = new_df_vol.reindex(columns=new_column_order)

    return new_df_vol

def union_vol(vol, vol_link, date_event, name_point):
    '''Объединяем два df, заменяем мусорный столбец с именами на нормальный'''
    for index, row in vol_link.iterrows():
        indices = vol.index[vol['Волонтёр'].str.contains(row['name_runner'])]
        for i in indices:
            vol.at[i, 'Волонтёр'] = row['name_runner']
            vol.at[i, 'link_runner'] = row['link_runner']
            vol.at[i, 'user_id'] = extract_user_id(row['link_runner'])
            vol.at[i, 'date_event'] = date_event
            vol.at[i, 'name_point'] = name_point
    vol['date_event'] = pd.to_datetime(vol['date_event'], format='%d.%m.%Y')
    return vol

def main_parse(link):
    #link = 'https://5verst.ru/aleksandrino/results/08.04.2023/'
    #link = 'https://5verst.ru/volgogradpanorama/results/07.09.2024/'  # Нет волонтерства, только протокол бегунов

    df_run, df_vol, df_run_link, df_vol_link, date_event, name_point = parse_protocol(link)
    final_df_run = processing_run(df_run, df_run_link, date_event, name_point)
    if isinstance(df_vol, pd.DataFrame):  # Проверяем тип данных в таблице с волонтёрами
        final_df_vol = processing_vol(df_vol, df_vol_link, date_event, name_point)
    return final_df_run, final_df_vol