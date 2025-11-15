import concurrent.futures
import configparser
import pandas as pd
import sqlalchemy as sa
from tqdm.auto import tqdm
from pathlib import Path
import parse_protocol as pp

'''Ниже реализация парсинга протоколов пробежек в несколько потоков'''

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
CONFIG_PATH = PROJECT_ROOT / "5_verst.ini"

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

db_host = config['five_verst_stats']['host']
db_user = config['five_verst_stats']['username']
db_pass = config['five_verst_stats']['password']
db_name = config['five_verst_stats']['dbname']

credential = f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}'

engine = sa.create_engine(credential)
result = pd.read_sql(f"SELECT * FROM list_all_events", con=engine)

all_protocol, all_protocol_vol = pd.DataFrame(), pd.DataFrame()

def process_link(row):
    link = row['link_event']
    final_df_run, final_df_vol = pp.main_parse(link)
    return final_df_run, final_df_vol

def tqdm_as_completed(futures, desc=None):
    """
    Функция для отображения прогресса выполнения асинхронных задач с использованием tqdm.
    """
    total = len(futures)
    pbar = tqdm(total=total, desc=desc)
    results = []
    for future in concurrent.futures.as_completed(futures):
        results.append(future.result())
        pbar.update(1)
    pbar.close()
    return results

MAX_WORKERS = 4  # Задаем максимальное количество потоков

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = []
    count = len(result)
    for _, row in result.iterrows():
        future = executor.submit(process_link, row)
        futures.append(future)

    tqdm_as_completed(futures, desc="Processing links")

    for final_df_run, final_df_vol in tqdm_as_completed(futures, desc="Concatenating results"):
        all_protocol = pd.concat([all_protocol, final_df_run], ignore_index=True)
        all_protocol_vol = pd.concat([all_protocol_vol, final_df_vol], ignore_index=True)