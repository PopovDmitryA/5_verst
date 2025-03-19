import configparser
import pandas as pd
from tqdm import tqdm
import sqlalchemy as sa
import parse_protocol as pp

config = configparser.ConfigParser()
config.read('/Users/dmitry/PycharmProjects/5_verst/5_verst.ini')

db_host = config['five_verst_stats']['host']
db_user = config['five_verst_stats']['username']
db_pass = config['five_verst_stats']['password']
db_name = config['five_verst_stats']['dbname']

credential = f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}'

engine = sa.create_engine(credential)
result = pd.read_sql(f"SELECT * FROM list_all_events", con=engine)

all_protocol, all_protocol_vol = pd.DataFrame(), pd.DataFrame()

count = len(result)
for index, row in tqdm(result.iterrows(), total=count):
    # Две строчки ниже расчитаны на первичный сбор данных через юпитер блокнот на случай, если скрипт падает, чтобы он продолжил заполнять df, который уже начал
    #if ((all_protocol['date_event'].isin([row['date_event']])) & (all_protocol['name_point'].isin([row['name_point']]))).any():
    #    continue
    link = row['link_event']
    final_df_run, final_df_vol = pp.main_parse(link)
    all_protocol = pd.concat([all_protocol, final_df_run], ignore_index=True)
    all_protocol_vol = pd.concat([all_protocol_vol, final_df_vol], ignore_index=True)