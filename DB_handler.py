import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import pandas as pd


def db_connect(credential):
    '''Подключение в базе данных'''
    engine = sa.create_engine(credential)
    return engine


def get_table(engine, name_table, columns='*'):
    '''Считать таблицу из БД'''
    table = pd.read_sql(f"SELECT {columns} FROM {name_table}", con=engine)
    return table


def append_df(engine, table, df):
    '''Добавление df в таблицу БД'''
    df.to_sql(table, engine, if_exists="append", index=False)
    info_table_update(engine, table, datetime.now())


def info_table_update(engine, table_name, upd_time):
    '''Функция записи информации об обновлении данных в определенной таблице БД (логер)'''
    Session = sessionmaker(bind=engine)
    session = Session()

    insert_query = sa.text(f"""
        INSERT INTO update_table (table_name, update_date)
        VALUES ('{table_name}', '{upd_time}');
        """)
    session.execute(insert_query)
    session.commit()
    session.close()

def update_data_from_df(engine, table_name, df, condition_columns, value_columns):
    '''В разработке'''
    Session = sessionmaker(bind=engine)
    session = Session()

    insert_query = sa.text(f"""
            UPDATE {table_name} 
            SET {value}
            WHERE {condition};
            """)
    session.execute(insert_query)
    session.commit()
    session.close()

def update_view(engine, view):
    '''Обновление материализованной view'''
    with engine.begin() as conn:
        query = sa.text(f"REFRESH MATERIALIZED VIEW {view}")
        conn.execute(query)