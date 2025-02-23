import pandas as pd
import requests, re
from bs4 import BeautifulSoup

def identification_park_date(link, t):
    '''Определяем название парка и дату пробежки'''
    date_event = link.split('/')[5]
    soup = BeautifulSoup(t.text, 'html.parser').find('div', class_='page-header page-results-header')
    for link_run in soup.find_all('h1'):
        k = link_run.text
    name_point = k.split('Протокол 5 вёрст')[1].split('(')[0].strip()
    return date_event, name_point

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
    except:  # IndexError:
        return None

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

def processing_vol(df_vol, date_event, name_point):
    '''Формируем финальный формат df волонтёров для БД'''
    df_vol_copy = df_vol.copy()
    df_vol_copy['date_event'] = date_event
    df_vol_copy['date_event'] = pd.to_datetime(df_vol_copy['date_event'], format='%d.%m.%Y')
    df_vol_copy['name_point'] = name_point
    df_vol_copy['user_id'] = df_vol_copy['link_runner'].apply(extract_user_id)
    df_vol_copy = df_vol_copy.drop(columns=['Участник'])

    new_column_order = ['name_point',
                        'date_event',
                        'name_runner',
                        'link_runner',
                        'user_id',
                        'vol_role']
    df_vol_copy = df_vol_copy.reindex(columns=new_column_order)

    return df_vol_copy

def parse_vol(t):
    '''Парсинг волонтеров со ссылками'''
    soup_vol = BeautifulSoup(t.text, 'html.parser')
    table_vol = soup_vol.find('table',
                              {'class': 'sortable n-last results-table min-w-full leading-normal'})
    rows = table_vol.find_all('tr')
    data = []
    for index, row in enumerate(rows):
        cols = row.find_all('td')
        if len(cols) == 0: continue

        cols = [col.text.strip() for col in cols]
        try:
            find_link = row.find('a')
            name_runner, link_runner = find_link.text, find_link.get('href')
            cols += [name_runner, link_runner]
        except:
            pass
        data.append(cols)
    columns = ['Участник',
               'vol_role',
               'name_runner',
               'link_runner']
    df_vol = pd.DataFrame(data, columns=columns)

    return df_vol

def processing_run(df_run_link, date_event, name_point):
    '''Формируем финальный формат df пробежки для БД'''
    df_run_copy = df_run_link.copy()
    df_run_copy['age_category'] = df_run_copy['Возрастной рейтинг'].apply(slice_before_parenthesis)
    df_run_copy.drop(columns=['Возрастной рейтинг'], inplace=True)
    df_run_copy['date_event'] = date_event
    df_run_copy['date_event'] = pd.to_datetime(df_run_copy['date_event'], format='%d.%m.%Y')
    df_run_copy['name_point'] = name_point
    df_run_copy['user_id'] = df_run_copy['link_runner'].apply(extract_user_id)
    mask = df_run_copy['Участник'] == 'НЕИЗВЕСТНЫЙ'
    df_run_copy.loc[mask, 'name_runner'] = df_run_copy.loc[mask, 'Участник']
    df_run_copy = check_status_runner(df_run_copy)
    df_run_copy = df_run_copy.drop(columns=['Участник'])

    new_column_order = ['name_point',
                        'date_event',
                        'name_runner',
                        'link_runner',
                        'user_id',
                        'position',
                        'finish_time',
                        'age_category',
                        'status_runner']
    df_run_copy = df_run_copy.reindex(columns=new_column_order)

    return df_run_copy

def parse_runner(t):
    '''Парсинг таблицы с бегунами'''
    soup_run = BeautifulSoup(t.text, 'html.parser')
    table_runner = soup_run.find('table',
                                 {
                                     'class': 'sortable n-last results-table results-table_with-sticky-head min-w-full leading-normal'})
    rows = table_runner.find_all('tr')
    data = []
    for index, row in enumerate(rows):
        cols = row.find_all('td')
        if len(cols) == 0: continue

        cols = [col.text.strip() for col in cols]
        try:
            find_link = row.find('a')
            name_runner, link_runner = find_link.text, find_link.get('href')
            cols += [name_runner, link_runner]
        except:
            pass
        data.append(cols)

    columns = ['position',
               'Участник',
               'Возрастной рейтинг',
               'finish_time',
               'name_runner',
               'link_runner']
    df_run_link = pd.DataFrame(data, columns=columns)

    return df_run_link

def parse_protocol(link):
    '''Функция, возвращающая по ссылке на протокол 3 DF со страницы протоколов'''
    parse_site = pd.read_html(link)  # Парсим табличку
    t = requests.get(link)  # Парсим страницу

    date_event, name_point = identification_park_date(link, t)

    if len(parse_site) == 1:  # Если волонтерства не отгружены
        df_run = parse_runner(t)
        df_vol = False
    else:
        # df_vol = pd.DataFrame(parse_site[1])
        df_run = parse_runner(t)
        df_vol = parse_vol(t)

    return df_run, df_vol, date_event, name_point

def main_parse(link):
    '''Главная функция, которая собирает по частям итоговый протокол'''
    df_run_link, df_vol_link, date_event, name_point = parse_protocol(link)
    final_df_run = processing_run(df_run_link, date_event, name_point)
    if isinstance(df_vol_link, pd.DataFrame):  # Проверяем тип данных в таблице с волонтёрами
        final_df_vol = processing_vol(df_vol_link, date_event, name_point)
        return final_df_run, final_df_vol
    return final_df_run, None