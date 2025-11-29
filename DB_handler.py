import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import pandas as pd
import time
import psycopg2

def retry_db(max_retries=3, delay=10):
    """
    Декоратор для автоповторов при ошибках соединения с PostgreSQL
    (OperationalError от SQLAlchemy или psycopg2).

    После ошибки:
    - выводит лог
    - делает dispose() коннекта (сброс пула)
    - ждёт delay секунд
    - повторяет попытку до max_retries раз

    Если все попытки неудачны → возвращает None.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            attempt = 1
            engine = None

            # Пытаемся найти engine в аргументах
            for a in args:
                if hasattr(a, "dispose"):
                    engine = a
                    break
            if not engine:
                engine = kwargs.get("engine", None)

            while attempt <= max_retries:
                try:
                    return func(*args, **kwargs)

                except (sa.exc.OperationalError, psycopg2.OperationalError) as e:
                    print(f"⚠️ Ошибка соединения с БД в {func.__name__} "
                          f"(попытка {attempt}/{max_retries}): {e}")

                    if engine:
                        try:
                            engine.dispose()
                        except:
                            pass

                    if attempt == max_retries:
                        print(f"❌ Не удалось выполнить {func.__name__} после {max_retries} попыток.")
                        return None

                    time.sleep(delay)
                    attempt += 1
        return wrapper
    return decorator

def db_connect(credential):
    '''Подключение к базе данных с проверкой живости соединения.'''
    engine = sa.create_engine(
        credential,
        pool_pre_ping=True,  # перед использованием соединения проверяет, не умерло ли оно
    )
    return engine

@retry_db()
def get_table(engine, name_table, columns='*'):
    '''Считать таблицу из БД'''
    query = f"SELECT {columns} FROM {name_table};"
    return pd.read_sql_query(query, con=engine)

@retry_db()
def execute_request(engine, sql_request):
    """
    Универсальная функция:
    - для SELECT-запросов возвращает pandas.DataFrame
    - для остальных запросов просто выполняет их и возвращает None
    """
    sql_str = str(sql_request).strip()
    is_select = sql_str.lower().startswith("select")

    if is_select:
        # чтение данных
        return pd.read_sql_query(sql_str, con=engine)
    else:
        # любые DDL/DML-операции
        with engine.connect() as conn:
            conn.execute(sa.text(sql_request))
            conn.commit()
        return None

@retry_db()
def append_df(engine, table_name, df):
    """
    Добавляет df в таблицу.
    to_sql уже внутри использует connection pool → ретрай через декоратор работает.
    """
    if df is None or df.empty:
        return

    df.to_sql(table_name, engine, if_exists='append', index=False)

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

@retry_db()
def get_inf_with_condition(engine, name_table, condition):
    """
        Функция считывает данные из PostgreSQL с заданными условиями.

        :param engine: объект подключения к базе данных
        :param name_table:return: строка с именем таблицы
        :param condition: список словарей вида {'column_name': 'value'}
        :return: таблица pandas.DataFrame с результатом выборки
        """
    where_conditions = create_condition(condition, 'AND')
    query = f"SELECT * FROM {name_table} WHERE {where_conditions};"
    return pd.read_sql_query(query, con=engine)

@retry_db()
def update_view(engine, view_name):
    '''Обновление материализованной view'''
    with engine.connect() as conn:
        conn.execute(sa.text(f"REFRESH MATERIALIZED VIEW {view_name};"))
        conn.commit()