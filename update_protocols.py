from sqlalchemy import Column, Integer, String, Boolean, DateTime, Time, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import sqlalchemy as sa
import DB_handler as db

def update_data_protocols(credential, for_removal_runner, for_removal_vol, to_add_runner, to_add_vol, different_list_of_protocols=[]):
    '''Кастомная функция удаления и записи новых данных в разные таблицы
    Логика такая, что делаем все внутри одной функции, чтобы если на каком-то этапе произойдет ошибка, то коммит не произойдет и частичные изменения не сохранятся'''
    engine = sa.create_engine(credential)
    Session = sessionmaker(bind=engine)
    session = Session()

    if len(different_list_of_protocols) != 0:
        #удаление с листа протоколов
        #count = len(different_list_of_protocols)
        print('Удаляем неактуальные протоколы')
        #for _, row in tqdm(different_list_of_protocols.iterrows(), total=count):
        for _, row in different_list_of_protocols.iterrows():
            list_сondition = [{'name_point': row['name_point']}, {'date_event': row['date_event']}]
            сondition = db.create_condition(list_сondition, 'AND')
            request = sa.text(f"""DELETE FROM list_all_events WHERE {сondition};""")
            session.execute(request)

    #удаление данных из протоколов пробежек
    #count = len(for_removal_runner)
    print('Удаляем неактуальные данные из протоколов пробежек')
    for _, row in for_removal_runner.iterrows():
        list_сondition_runner = [{'name_point': row['name_point']}, {'date_event': row['date_event']}, {'position': row['position']}]
        сondition_runner = db.create_condition(list_сondition_runner, 'AND')
        request_runner = sa.text(f"""   DELETE FROM details_protocol
                                        WHERE {сondition_runner};""")
        session.execute(request_runner)

    #удаление данных о волонтерах из протоколов
    #count = len(for_removal_vol)
    print('Удаляем неактуальные данные о волонтёрах')
    for _, row in for_removal_vol.iterrows():
        list_сondition_vol = [{'name_point': row['name_point']},
                          {'date_event': row['date_event']},
                          {'user_id': row['user_id']},
                          {'vol_role': row['vol_role']}]
        сondition_vol = db.create_condition(list_сondition_vol, 'AND')
        request_vol = sa.text(f"""  DELETE FROM details_vol
                                    WHERE {сondition_vol};""")
        session.execute(request_vol)

    # Реализуем запись данных в таблицу базы данных
    # Модель для таблицы list_all_events
    Base = declarative_base()

    if len(different_list_of_protocols) != 0:
        class ListEvents(Base):
            __tablename__ = 'list_all_events'

            index_event = Column(Integer)
            name_point = Column(String)
            date_event = Column(DateTime)
            link_event = Column(String)
            is_test = Column(Boolean)
            count_runners = Column(Integer)
            count_vol = Column(Integer)
            mean_time = Column(DateTime)
            best_time_woman = Column(DateTime)
            best_time_man = Column(DateTime)
            updated_at = Column(DateTime)

            __table_args__ = (
                PrimaryKeyConstraint('index_event', 'name_point', 'date_event'),
            )

        # Преобразуем строки DataFrame в объекты ORM
        objects_to_insert_list_event = []
        for _, row in different_list_of_protocols.iterrows():
            obj = ListEvents(
                index_event=row['index_event'],
                name_point=row['name_point'],
                date_event=row['date_event'],
                link_event=row['link_event'],
                is_test=row['is_test'],
                count_runners=row['count_runners'],
                count_vol=row['count_vol'],
                mean_time=row['mean_time'],
                best_time_woman=row['best_time_woman'],
                best_time_man=row['best_time_man']
            )
            objects_to_insert_list_event.append(obj)

        # Запись в базу данных
        session.bulk_save_objects(objects_to_insert_list_event)

    # Дальше запись в БД протоколов бегунов
    class DetailsProtocolRun(Base):
        __tablename__ = 'details_protocol'

        name_point = Column(String)
        date_event = Column(DateTime)
        name_runner = Column(String)
        link_runner = Column(String)
        user_id = Column(String)
        position = Column(Integer)
        finish_time = Column(Time)
        age_category = Column(String)
        status_runner = Column(String)
        updated_at = Column(DateTime)

        __table_args__ = (
            PrimaryKeyConstraint('name_point', 'date_event', 'position'),
        )

    # Преобразуем строки DataFrame в объекты ORM
    objects_to_insert_run = []
    for _, row in to_add_runner.iterrows():
        obj = DetailsProtocolRun(
            name_point=row['name_point'],
            date_event=row['date_event'],
            name_runner=row['name_runner'],
            link_runner=row['link_runner'],
            user_id=row['user_id'],
            position=row['position'],
            finish_time=row['finish_time'],
            age_category=row['age_category'],
            status_runner=row['status_runner']
        )
        objects_to_insert_run.append(obj)

    # Массивная запись в базу данных
    session.bulk_save_objects(objects_to_insert_run)

    #Дальше запись в БД протоколов волонтеров
    class DetailsProtocolVol(Base):
        __tablename__ = 'details_vol'

        name_point = Column(String)
        date_event = Column(DateTime)
        name_runner = Column(String)
        link_runner = Column(String)
        user_id = Column(String)
        vol_role = Column(String)
        updated_at = Column(DateTime)

        __table_args__ = (
            PrimaryKeyConstraint('name_point', 'date_event', 'user_id', 'vol_role'),
        )

    # Преобразуем строки DataFrame в объекты ORM
    objects_to_insert_vol = []
    for _, row in to_add_vol.iterrows():
        obj = DetailsProtocolVol(
            name_point=row['name_point'],
            date_event=row['date_event'],
            name_runner=row['name_runner'],
            link_runner=row['link_runner'],
            user_id=row['user_id'],
            vol_role=row['vol_role']
        )
        objects_to_insert_vol.append(obj)

    # Массивная запись в базу данных
    session.bulk_save_objects(objects_to_insert_vol)

    session.execute(sa.text("REFRESH MATERIALIZED VIEW new_turists"))
    session.execute(sa.text("REFRESH MATERIALIZED VIEW new_turists_vol"))
    session.commit()
    session.close()
