from datetime import datetime
import sqlalchemy as sa
import pandas as pd
import logging

# Настройка логгера
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Создание файла для записи логов
handler = logging.FileHandler('logfile.log')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def update_date_load():
    logger.info("Запуск функции your_function()")
    credential = 'postgresql://popov:lemon.014789@195.58.34.112:5432/five_verst_stats'
    # Парсим табличку со страницы последних протоколов без ссылок, но все столбцы
    site = r'https://5verst.ru/results/latest/'
    last_event = pd.read_html(site)
    logger.info('Считали данные с сайта')
    # Получение текущей даты и времени
    current_datetime = datetime.now()

    last_event = pd.DataFrame(last_event[0])
    last_event['date_type_event'] = last_event['Дата'].copy()
    last_event['date_type_event'] = pd.to_datetime(last_event['date_type_event'], format='%d.%m.%Y')
    last_event['name_point'] = last_event['Старт #'].str.split(' #').str[0]

    # Соединение с базой данных
    engine = sa.create_engine(credential)
    result = pd.read_sql(f"SELECT * FROM general_date_load_protocol", con=engine)
    logger.info('Подключились к БД и считали данные из таблицы')
    # чеккер
    test_count = 0
    new_protocol = 0
    logger.info('Начинаем сравнение данных с БД')
    for index, row in last_event.iterrows():
        name_point = row['name_point']
        if '#' not in row['Старт #']:
            k = row['Старт #']
            #print(f'Тестовый забег: {k}')
            test_count += 1
            continue
        date_event = row['date_type_event']
        exists = ((result['name_point'] == name_point) & (result['date_event'] == date_event)).any()  # Проверка, что в БД уже есть строчка с этим названием и датой
        if not exists:
            # здесь реализовал запись в БД факта выкладывания нового протокола

            with engine.begin() as connection:
                # SQL-запрос для вставки с проверкой на конфликт
                insert_query = sa.text("""
                INSERT INTO general_date_load_protocol (name_point, date_event, date_load)
                VALUES (:name_point, :date_event, :date_load)
                ON CONFLICT (name_point, date_event) DO UPDATE 
                SET name_point = EXCLUDED.name_point, 
                    date_event = EXCLUDED.date_event;
                """)
                connection.execute(insert_query, {'name_point': name_point,
                                                  'date_event': date_event,
                                                  'date_load': current_datetime})
                #print(result2)
            new_protocol += 1
            logger.info(f'Для парка {name_point} протокол от {date_event} записан в {current_datetime}')


    # if new_protocol == 0:
    #     connection = engine.connect()
    #     # Запрос на запись в БД данных
    #     insert_query = """
    #     INSERT INTO update_table (table_name, update_date)
    #     VALUES (%s, %s);
    #     """
    #     # Данные для вставки
    #     values_to_insert = ('general_date_load_protocol', current_datetime)
    #     # Выполнение запроса
    #     connection.execute(insert_query, values_to_insert)

try:
    update_date_load()
except Exception as e:
    logger.info(f'Ошибка {e}')
finally:
    logger.info('Завершение работы скрипта')
    logger.info('--------------------------------')