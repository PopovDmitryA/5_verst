import pandas as pd
import requests, re
from bs4 import BeautifulSoup
import time
import logging
import cloudscraper
import random
from io import StringIO

UA_HDRS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/127.0.0 Safari/537.36"),
    "Accept-Language": "ru,en;q=0.9",
    "Referer": "https://5verst.ru/",
}

logger = logging.getLogger(__name__)

RETRY_HTTP_CODES = {500, 502, 503, 504}

scraper = cloudscraper.create_scraper(
    browser={
        "browser": "chrome",
        "platform": "windows",
        "mobile": False
    }
)

def _sleep_with_jitter(base_sleep: float, jitter_factor: float = 0.1):
    """
    Пауза с мягким джиттером.
    base_sleep – базовая задержка в секундах
    jitter_factor – доля разброса (0.1 = ±10%)
    """
    low = 1.0 - jitter_factor
    high = 1.0 + jitter_factor
    k = random.uniform(low, high)
    sleep_for = base_sleep * k
    logger.info(
        f"Задержка перед повтором: {sleep_for:.1f} сек "
        f"(база {base_sleep:.1f}, джиттер {k:.2f})"
    )
    time.sleep(sleep_for)


def _fetch_html(
    url: str,
    retries: int = 6,
    timeout: int = 50,
    backoff_factor: int = 2,
):
    last_exc = None

    for attempt in range(1, retries + 1):
        try:
            logger.info(
                f"Запрос к {url}, попытка {attempt}/{retries}, timeout={timeout}s"
            )
            r = scraper.get(url, timeout=timeout)
            r.raise_for_status()
            return r.text

        except requests.exceptions.ReadTimeout as e:
            # сеть тупит → повторяем
            last_exc = e
            if attempt == retries:
                logger.error(f"Таймаут на {url} после {retries} попыток.")
                break

            base_sleep = timeout * (backoff_factor ** (attempt - 1))
            logger.warning(
                f"Таймаут при запросе {url}: {e}. "
                f"Базовая задержка перед повтором {base_sleep} сек..."
            )
            _sleep_with_jitter(base_sleep)
            # и идём на следующую попытку

        except requests.exceptions.ConnectionError as e:
            # обрыв соединения → повторяем
            last_exc = e
            if attempt == retries:
                logger.error(f"Ошибка соединения на {url} после {retries} попыток.")
                break

            base_sleep = timeout * (backoff_factor ** (attempt - 1))
            logger.warning(
                f"Ошибка соединения {url}: {e}. "
                f"Базовая задержка перед повтором {base_sleep} сек..."
            )
            _sleep_with_jitter(base_sleep)

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code
            last_exc = e

            if status in RETRY_HTTP_CODES:
                # временные проблемы у сервера → пробуем ещё
                if attempt == retries:
                    logger.error(f"HTTP {status} на {url} после {retries} попыток.")
                    break

                base_sleep = timeout * (backoff_factor ** (attempt - 1))
                logger.warning(
                    f"HTTP {status} при запросе {url}. "
                    f"Базовая задержка перед повтором {base_sleep} сек..."
                )
                _sleep_with_jitter(base_sleep)
                continue

            # остальные HTTP ошибки не лечатся ретраями
            logger.exception(f"HTTP ошибка при запросе {url}: {e}")
            break

        except Exception as e:
            last_exc = e
            logger.exception(f"Неожиданная ошибка при запросе {url}: {e}")
            break

    raise RuntimeError(f"Не удалось получить HTML: {last_exc}")

def _is_valid_protocol_page(soup) -> bool:
    """
    Проверяем, что страница действительно похожа на страницу протокола:
    - есть div.page-header.page-results-header
    - внутри есть h1 с текстом 'Протокол 5 вёрст'
    """
    header_div = soup.find('div', class_='page-header page-results-header')
    if not header_div:
        return False

    h1 = header_div.find('h1')
    if not h1:
        return False

    text = h1.get_text(' ', strip=True)
    if 'Протокол 5 вёрст' not in text:
        return False

    return True

def identification_park_date(link, soup):
    """Определяем название парка и дату пробежки"""
    # дата забега из ссылки вида .../results/29.11.2025/
    date_event = link.split('/')[5]

    header_div = soup.find('div', class_='page-header page-results-header')
    if not header_div:
        raise ValueError(f"Не найден блок заголовка протокола на странице {link}")

    h1 = header_div.find('h1')
    if not h1:
        raise ValueError(f"Не найден заголовок h1 протокола на странице {link}")

    text = h1.get_text(' ', strip=True)

    if 'Протокол 5 вёрст' not in text:
        raise ValueError(
            f"Неожиданный текст заголовка протокола на {link}: {text!r}"
        )

    # text примерно: "Протокол 5 вёрст <парка> (<город>) за 29.11.2025"
    middle = text.split('Протокол 5 вёрст', 1)[1]
    name_point = middle.split('(')[0].strip()

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

    def determine_status(row):
        if row['name_runner'] == 'НЕИЗВЕСТНЫЙ':
            return 'unknown_runner'
        elif pd.isna(row['user_id']):
            return 'unregistered_runner'
        else:
            return 'active_runner'

    new_df_run['status_runner'] = new_df_run.apply(determine_status, axis=1)

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

def parse_vol(soup):
    """Парсинг волонтеров, возврат DataFrame"""
    table_vol = soup.find(
        'table',
        {'class': 'sortable n-last results-table min-w-full leading-normal'}
    )
    if not table_vol:
        return pd.DataFrame()

    rows = table_vol.find_all('tr')
    data = []
    for row in rows:
        cols = row.find_all('td')
        if not cols:
            continue
        cols = [col.text.strip() for col in cols]
        find_link = row.find('a')
        if find_link:
            name_runner, link_runner = find_link.text, find_link.get('href')
            cols += [name_runner, link_runner]
        data.append(cols)

    columns = ['Участник', 'vol_role', 'name_runner', 'link_runner']
    return pd.DataFrame(data, columns=columns)

def processing_run(df_run_link, date_event, name_point):
    '''Формируем финальный формат df пробежки для БД'''
    df_run_copy = df_run_link.copy()
    # Вычленение возрастной группы из мусорной строки и удаление ненужного столбца
    df_run_copy['age_category'] = df_run_copy['Возрастной рейтинг'].apply(slice_before_parenthesis)
    df_run_copy.drop(columns=['Возрастной рейтинг'], inplace=True)

    # Добавление даты события и наименования точки
    df_run_copy['date_event'] = pd.to_datetime(date_event, format='%d.%m.%Y')
    df_run_copy['name_point'] = name_point

    # Извлечение user_id из ссылки
    df_run_copy['user_id'] = df_run_copy['link_runner'].apply(extract_user_id)

    # Заполнение пустых полей в случае неизвестного участника
    mask = df_run_copy['Участник'] == 'НЕИЗВЕСТНЫЙ'
    df_run_copy.loc[mask, 'name_runner'] = df_run_copy.loc[mask, 'Участник']

    # Проверка статуса участника и удаление лишнего столбца
    df_run_copy = check_status_runner(df_run_copy)
    df_run_copy = df_run_copy.drop(columns=['Участник'])

    df_run_copy = df_run_copy.reindex(columns=[
        'name_point', 'date_event', 'name_runner', 'link_runner', 'user_id',
        'position', 'finish_time', 'age_category', 'status_runner'
    ])

    df_run_copy['finish_time'] = df_run_copy['finish_time'].replace('', pd.NA).fillna('00:00:00')
    df_run_copy['finish_time'] = pd.to_datetime(df_run_copy['finish_time'], format='%H:%M:%S', errors='coerce').dt.time

    df_run_copy['position'] = pd.to_numeric(df_run_copy['position'], errors='coerce').astype('Int64')

    return df_run_copy

def parse_runner(soup):
    """Парсинг таблицы с бегунами и возврат сырого DataFrame"""
    table_runner = soup.find(
        'table',
        {'class': 'sortable n-last results-table results-table_with-sticky-head min-w-full leading-normal'}
    )
    if not table_runner:
        return pd.DataFrame()  # защита

    rows = table_runner.find_all('tr')
    data = []
    for row in rows:
        cols = row.find_all('td')
        if not cols:
            continue
        cols = [col.text.strip() for col in cols]
        find_link = row.find('a')
        if find_link:
            name_runner, link_runner = find_link.text, find_link.get('href')
            cols += [name_runner, link_runner]
        data.append(cols)

    columns = ['position', 'Участник', 'Возрастной рейтинг', 'finish_time', 'name_runner', 'link_runner']
    return pd.DataFrame(data, columns=columns)

def parse_protocol(link):
    """Возвращает сырые 2 DF со страницы с протоколом и дату с именем локации"""
    html = _fetch_html(link)
    soup = BeautifulSoup(html, 'lxml')

    # таблицы можно при желании оставить через read_html, если нужно что-то общее
    parse_site = pd.read_html(StringIO(html), flavor="lxml")

    date_event, name_point = identification_park_date(link, soup)

    if len(parse_site) == 1:
        df_run = parse_runner(soup)
        df_vol = False
    else:
        df_run = parse_runner(soup)
        df_vol = parse_vol(soup)

    return df_run, df_vol, date_event, name_point

def main_parse(link):
    '''Главная функция, которая собирает по частям итоговый протокол'''
    df_run_link, df_vol_link, date_event, name_point = parse_protocol(link)
    final_df_run = processing_run(df_run_link, date_event, name_point)
    if isinstance(df_vol_link, pd.DataFrame):  # Проверяем тип данных в таблице с волонтёрами
        final_df_vol = processing_vol(df_vol_link, date_event, name_point)
        return final_df_run, final_df_vol
    return final_df_run, None