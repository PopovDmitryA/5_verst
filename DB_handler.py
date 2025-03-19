import configparser
import sqlalchemy as sa
import pandas as pd

config = configparser.ConfigParser()
config.read('5_verst.ini')

db_host = config['five_verst_stats']['host']
db_user = config['five_verst_stats']['username']
db_pass = config['five_verst_stats']['password']
db_name = config['five_verst_stats']['dbname']

credential = f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}'

def db_connect(credential):
    '''Подключение в базе данных'''
    engine = sa.create_engine(credential)
    return engine

def get_table(engine, name_table, columns='*'):
    '''Считать таблицу из БД'''
    table = pd.read_sql(f"SELECT {columns} FROM {name_table}", con=engine)
    return table