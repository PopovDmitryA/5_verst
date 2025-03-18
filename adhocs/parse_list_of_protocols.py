import configparser
import pandas as pd
from tqdm import tqdm
import sqlalchemy as sa
import link_handler as lh
import parse_table_protocols_in_park as ptpp

config = configparser.ConfigParser()
config.read('/Users/dmitry/PycharmProjects/5_verst/5_verst.ini')

db_host = config['five_verst_stats']['host']
db_user = config['five_verst_stats']['username']
db_pass = config['five_verst_stats']['password']
db_name = config['five_verst_stats']['dbname']

credential = f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}'

engine = sa.create_engine(credential)
result = pd.read_sql(f"SELECT * FROM general_link_all_location", con=engine)

empty_df = pd.DataFrame()

count = len(result)
for index, row in tqdm(result.iterrows(), total=count):
    link = lh.link_all_result_event(row['link_point'])
    all_point_protocol = ptpp.transform_df_list_protocol(ptpp.list_protocols_in_park(link))
    empty_df = pd.concat([empty_df, all_point_protocol], ignore_index=True)
print(empty_df)