from sqlalchemy.exc import OperationalError

'''Ретрай при попытке записи в БД данных'''
def execute_with_retries(connection, query, MAX_RETRIES, params=None): #Здесь нужно добавить передачу в функцию количество попыток
    while True:
        try:
            return connection.execute(query, params)
        except OperationalError as e:
            if MAX_RETRIES <= 0 or 'Operation timed out' not in str(e):
                raise

            print(f"Произошла ошибка {e}. Осталось попыток: {MAX_RETRIES}")
            MAX_RETRIES -= 1
            continue

'''Функция для записи в таблицу лога факта обновления какой-либо таблицы'''
def add_logs(date_at, engine):
    with engine.connect() as connection:
        # Запрос на запись в БД данных
        insert_query = """
        INSERT INTO update_table (table_name, update_date)
        VALUES (%s, %s);
        """
        # Данные для вставки
        values_to_insert = ('general_link_all_events', date_at)
        # Выполнение запроса
        connection.execute(insert_query, values_to_insert)