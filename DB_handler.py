import sqlalchemy as sa
import pandas as pd

def db_connect(credential):
    '''Подключение в базе данных'''
    engine = sa.create_engine(credential)
    return engine

def get_table(engine, name_table, columns='*'):
    '''Считать таблицу из БД'''
    table = pd.read_sql(f"SELECT {columns} FROM {name_table}", con=engine)
    return table