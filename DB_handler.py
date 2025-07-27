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

def execute_request(engine, request):
    '''Выполнить целиком запрос'''
    result = pd.read_sql(request, con=engine)
    return result

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


def create_condition(condition, type):
    # формируем части условий для WHERE
    where_conditions = []
    for cond in condition:
        key = list(cond.keys())[0]
        value = cond[key]
        where_conditions.append(f"{key}='{value}'")
        # if isinstance(value, str):
        #     # Если значение является строкой, добавляем одинарные кавычки вокруг значения
        #     where_conditions.append(f"{key}='{value}'")
        # else:
        #     # Для чисел и прочих типов используем значение напрямую
        #     where_conditions.append(f"{key}={value}")
    # Формируем общий запрос с условием WHERE
    where_clause = f" {type} ".join(where_conditions)
    return where_clause

def get_inf_with_condition(engine, name_table, condition):
    """
    Функция считывает данные из PostgreSQL с заданными условиями.

    :param engine: объект подключения к базе данных
    :param name_table:return: строка с именем таблицы
    :param condition: список словарей вида {'column_name': 'value'}
    :return: таблица pandas.DataFrame с результатом выборки
    """
    # формируем части условий для WHERE
    where_conditions = create_condition(condition, 'AND')
    request = f"""
        SELECT * FROM {name_table}
        WHERE {where_conditions};
    """
    # Выполняем запрос и получаем результат в виде DataFrame
    try:
        df = pd.read_sql_query(request, con=engine)
        return df
    except Exception as e:
        print("Ошибка при выполнении запроса:", e)
        return None

def update_view(engine, view):
    '''Обновление материализованной view'''
    with engine.begin() as conn:
        query = sa.text(f"REFRESH MATERIALIZED VIEW {view}")
        conn.execute(query)