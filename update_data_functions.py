import DB_handler as db
import link_handler
import parse_last_running as plr
import parse_protocol as pp
import parse_table_protocols_in_park as ptpp
from update_protocols import update_data_protocols

import pandas as pd
import time
import random
from tqdm import tqdm

def check_new_protocols(credential):
    """Получаем данные протоколов, которые можно внести в БД"""
    engine = db.db_connect(credential)
    df = db.get_table(engine, 'list_all_events', 'index_event, name_point, date_event, link_event, is_test')
    df['link_point'] = df['link_event'].apply(link_handler.main_link_event)
    df = df.drop(columns=['link_event'])
    db_data = df[['index_event', 'name_point', 'date_event']].copy()

    # Парсим последние протоколы
    last_event = plr.transform_df_last_event(plr.last_event_parse())

    # Формируем таблицу для сравнения
    compare_event = last_event[['index_event', 'name_point', 'date_event']].copy()

    # Слияние и поиск отсутствующих строк
    merged_df = compare_event.merge(
        db_data,
        on=['index_event', 'name_point', 'date_event'],
        how='left',
        indicator=True
    )
    missing_rows = merged_df[merged_df['_merge'] == 'left_only'].copy()
    missing_rows = missing_rows.drop(columns=['_merge'])
    new_data = missing_rows.merge(
        last_event,
        on=['index_event', 'name_point', 'date_event'],
        how='left'
    )

    # Определяем, как называется колонка со ссылкой в last_event/new_data
    if 'link_event' in last_event.columns:
        link_col = 'link_event'
    elif 'link_point' in last_event.columns:
        link_col = 'link_point'
    else:
        raise KeyError("Не найдена колонка ссылки ни 'link_event', ни 'link_point' в last_event")

    # Проверяем, что аналогичная колонка есть в new_data
    if link_col not in new_data.columns:
        alt = 'link_point' if link_col == 'link_event' else 'link_event'
        if alt in new_data.columns:
            new_data = new_data.rename(columns={alt: link_col})
        else:
            print(f"⚠️ В new_data нет поля '{link_col}', создаём пустое")
            new_data[link_col] = None

    # Порядок столбцов
    column_order = [
        'index_event',
        'name_point',
        'date_event',
        link_col,
        'is_test',
        'count_runners',
        'count_vol',
        'mean_time',
        'best_time_woman',
        'best_time_man'
    ]
    column_order = [c for c in column_order if c in last_event.columns]

    # Ключевые поля
    keys = ['index_event', 'name_point', 'date_event', link_col, 'is_test']
    keys = [k for k in keys if k in new_data.columns and k in last_event.columns]

    # Финальный df для вставки в БД
    finish_df = (
        last_event[column_order]
        .merge(new_data[keys], on=keys, how='inner')
    )

    # сохраняем исходный df с сайта и получаем аналогичный из БД, чтобы их сравнить
    for_find_dif = last_event[column_order]
    frames = []

    for _, row in for_find_dif.iterrows():
        condition = [{'name_point': row['name_point']}, {'date_event': row['date_event']}]
        temp_now_protocol = db.get_inf_with_condition(engine, 'list_all_events', condition)

        if temp_now_protocol is None:
            continue

        if not temp_now_protocol.empty and not temp_now_protocol.isna().all(axis=None):
            frames.append(temp_now_protocol)

    # Объединяем один раз — без предупреждений и быстрее
    if frames:
        now_db_last_protocols = pd.concat(frames, ignore_index=True)
    else:
        now_db_last_protocols = pd.DataFrame()

    # Удаляем служебные колонки, если они есть
    if 'updated_at' in now_db_last_protocols.columns:
        now_db_last_protocols = now_db_last_protocols.drop(columns=['updated_at'])

    return finish_df, for_find_dif, now_db_last_protocols

def get_list_protocol(new_data):
    """
    В цикле проходимся по каждой строке df со ссылками на протоколы,
    парсим сами протоколы и собираем это в единую таблицу.
    После каждой итерации добавляется случайная задержка от 10 до 20 секунд.
    """
    data_protocols, data_protocol_vol = pd.DataFrame(), pd.DataFrame()

    for _, row in new_data.iterrows():
        link = row['link_event']
        final_df_run, final_df_vol = pp.main_parse(link)
        data_protocols = pd.concat([data_protocols, final_df_run], ignore_index=True)
        data_protocol_vol = pd.concat([data_protocol_vol, final_df_vol], ignore_index=True)

        print(f'\t{row["date_event"]} - {row["name_point"]}: '
              f'{row["count_runners"]} участников, {row["count_vol"]} волонтеров')

        # 💤 случайная задержка между запросами
        delay = random.uniform(10, 20)
        print(f'Пауза {delay:.1f} сек перед следующим протоколом...')
        time.sleep(delay)

    return data_protocols, data_protocol_vol

# def add_new_protocols(credential, new_data, data_protocols, data_protocol_vol):
#     '''Проверяем наличие протоколов, которые можно записать в БД, записываем + парсим детали протокола и записываем их в базу'''
#     #engine = db.db_connect(credential)
#     # new_data = check_new_protocols(credential)
#     # if len(new_data) == 0:
#     #     return
#     # print(f'Есть {len(new_data)} протоколов для записи в БД')
#     engine = db.db_connect(credential)
#     db.append_df(engine, 'list_all_events', new_data)
#     #data_protocols, data_protocol_vol = pd.DataFrame(), pd.DataFrame()
#
#     # for _, row in new_data.iterrows():
#     #     link = row['link_event']
#     #     final_df_run, final_df_vol = pp.main_parse(link)
#     #     data_protocols = pd.concat([data_protocols, final_df_run], ignore_index=True)
#     #     data_protocol_vol = pd.concat([data_protocol_vol, final_df_vol], ignore_index=True)
#     #     print(f'\t{row["date_event"]} - {row["name_point"]}: {row["count_runners"]} участников, {row["count_vol"]} волонтеров')
#
#     #engine = db.db_connect(credential)
#     db.append_df(engine, 'details_protocol', data_protocols)
#     db.update_view(engine, 'new_turists')
#
#     #engine = db.db_connect(credential)
#     db.append_df(engine, 'details_vol', data_protocol_vol)
#     db.update_view(engine, 'new_turists_vol')
#
#     return print(f'В БД Записано {len(new_data)} протоколов, {len(data_protocols)} строчек бегунов, {len(data_protocol_vol)} строчек волонтеров')

def add_new_protocols(credential, new_data, data_protocols, data_protocol_vol):
    """
    Записываем новые протоколы в БД так, чтобы по КАЖДОМУ протоколу
    атомарно обновлялись сразу три сущности:
      - list_all_events
      - details_protocol
      - details_vol

    new_data        — df для list_all_events (finish_df из check_new_protocols)
    data_protocols  — все бегуны по этим протоколам
    data_protocol_vol — все волонтёры по этим протоколам
    """

    if new_data is None or new_data.empty:
        print("Новых протоколов для записи нет.")
        return

    total_runners = 0
    total_vols = 0

    # Пройдёмся по каждому протоколу отдельно
    for _, row in new_data.iterrows():
        name_point = row["name_point"]
        date_event = pd.to_datetime(row["date_event"])

        # 1) Выделяем детали по бегунам/волонтёрам для конкретного протокола
        run_slice = data_protocols[
            (data_protocols["name_point"] == name_point) &
            (pd.to_datetime(data_protocols["date_event"]) == date_event)
        ].copy()

        vol_slice = data_protocol_vol[
            (data_protocol_vol["name_point"] == name_point) &
            (pd.to_datetime(data_protocol_vol["date_event"]) == date_event)
        ].copy()

        # Если по какой-то причине нет бегунов — лучше ничего не писать вообще
        if run_slice.empty:
            print(f"⚠️ Пропускаем протокол {name_point} / {date_event.date()} — нет данных по бегунам.")
            continue

        # 2) Формируем структуры под update_data_protocols как для НОВОГО протокола
        for_removal_runner = pd.DataFrame(columns=["name_point", "date_event", "position"])
        for_removal_vol = pd.DataFrame(columns=["name_point", "date_event", "user_id", "vol_role"])

        to_add_runner = run_slice

        if vol_slice.empty:
            to_add_vol = pd.DataFrame(
                columns=["name_point", "date_event", "name_runner", "link_runner", "user_id", "vol_role"]
            )
        else:
            to_add_vol = vol_slice

        # Строка для list_all_events — один протокол = одна строка
        different_list_of_protocols = row.to_frame().T

        # 3) Один вызов update_data_protocols → одна транзакция на три таблицы
        update_data_protocols(
            credential,
            for_removal_runner, for_removal_vol,
            to_add_runner, to_add_vol,
            different_list_of_protocols
        )

        total_runners += len(to_add_runner)
        total_vols += len(to_add_vol)

        print(f"\tЗаписан новый протокол: {name_point} — {date_event.date()}")

    print(
        f"В БД записано {len(new_data)} протоколов, "
        f"{total_runners} строчек бегунов, {total_vols} строчек волонтёров"
    )

def get_list_all_protocol(credential):
    '''Функция собирает данные со страниц всех протоколов каждого парка + получает из БД аналогичную таблицу, чтобы потом сравнить'''
    engine = db.db_connect(credential)
    # Берем ссылки на локации из БД и таблицу, с которой в дальнейшем будем сравнивать собранный ниже df
    request = 'SELECT general_link_all_location.* FROM general_link_all_location join general_location gl using (name_point) where is_pause = false'
    result = db.execute_request(engine, request)
    #result = result.iloc[121:] #Если нужно обновить данные по частям

    #Часть ниже реализована для запуска скрипта не на весь пул протоколов, а на часть
    where_conditions = []
    for _, row in result.iterrows():
        value = row['name_point']
        where_conditions.append(f"name_point ='{value}'")
    where_clause = " OR ".join(where_conditions)

    request_now = f'SELECT list_all_events.* FROM list_all_events join general_location gl using (name_point) where is_pause = false and {where_clause}'

    table = db.execute_request(engine, request_now)
    table = table.drop(columns=['updated_at'])
    empty_df = pd.DataFrame()

    count = len(result)
    # Собираем в единый df все данные со списком протоколов каждой локации
    for _, row in tqdm(result.iterrows(), total=count):
        link = link_handler.link_all_result_event(row['link_point'])
        all_point_protocol = ptpp.transform_df_list_protocol(ptpp.list_protocols_in_park(link))
        empty_df = pd.concat([empty_df, all_point_protocol], ignore_index=True)

        # Задержка от 10 до 20 секунд
        delay = random.uniform(10, 20)
        time.sleep(delay)
    #print(len(empty_df), len(table))
    print('Спарсили списки всех протоколов для сравнения')
    return empty_df, table

def find_dif_list_protocol(list_site_protocols, now_table):
    """
    Функция сравнивает два DataFrame и выводит значения из list_site_protocols,
    которые отсутствуют в now_table (эти протоколы ещё не обработаны),
    а затем возвращает строки, которые требуют обновления.
    """

    # Нормализуем формат дат для надёжного merge
    list_site_protocols = list_site_protocols.copy()
    now_table = now_table.copy()

    list_site_protocols['date_event'] = pd.to_datetime(list_site_protocols['date_event'], errors='coerce')
    now_table['date_event'] = pd.to_datetime(now_table['date_event'], errors='coerce')

    # Шаг 1. Находим протоколы, которых ещё нет в БД → их нужно исключить из сравнения
    temp_list = list_site_protocols[['name_point', 'date_event']]
    temp_now = now_table[['name_point', 'date_event']]

    diff_right_new = temp_list.merge(
        temp_now,
        on=['name_point', 'date_event'],
        how='left',
        indicator=True
    )

    missing = diff_right_new.query('_merge == "left_only"')[['name_point', 'date_event']]

    if not missing.empty:
        print('Есть протокол, не записанный в БД')
        print(missing)

        # Удаляем эти строки по значениям, НЕ по индексу
        list_site_protocols = list_site_protocols.merge(
            missing,
            on=['name_point', 'date_event'],
            how='left',
            indicator=True
        ).query('_merge == "left_only"').drop(columns=['_merge']).reset_index(drop=True)

    # Шаг 2. Теперь ищем строки, которые есть на сайте, но отличаются в БД → их нужно обновить
    diff_right = list_site_protocols.merge(now_table, how='left', indicator=True)
    diff_right = diff_right.query('_merge == "left_only"').drop(columns=['_merge'])

    return diff_right.sort_values(by=['date_event', 'name_point'], ascending=True).reset_index(drop=True)

def get_now_protocols(credential, different_list_of_protocols):
    '''Функция собирает 2 df с текущей информацией из протоколов, которая находится в БД'''
    engine = db.db_connect(credential)

    result_run, result_vol = pd.DataFrame(), pd.DataFrame()
    for _, row in different_list_of_protocols.iterrows():
        сondition = [{'name_point': row['name_point']}, {'date_event': row['date_event']}] #Формируем условие
        temp_result_run = db.get_inf_with_condition(engine, 'details_protocol', сondition)
        temp_result_vol = db.get_inf_with_condition(engine, 'details_vol', сondition)
        result_run = pd.concat([result_run, temp_result_run], ignore_index=True)
        # Проверяем наличие данных в temp_result_vol перед конкатенацией
        if not temp_result_vol.empty and not temp_result_vol.isna().all(axis=None):
            result_vol = pd.concat([result_vol, temp_result_vol], ignore_index=True)
        else:
            # Можно оставить сообщение для отладки или вообще ничего не делать
            pass

    result_run = result_run.drop(columns=['updated_at'])
    if len(result_vol) != 0:
        result_vol = result_vol.drop(columns=['updated_at'])

    return result_run, result_vol

def find_dif_protocol(actual_df, not_actual_df):
    '''Функция ищет отличия в двух датафреймах и возвращает df для добавления в БД и df со строчками для удаления из БД данных'''
    diff_left = not_actual_df.merge(actual_df, how='left', indicator=True)
    diff_right = not_actual_df.merge(actual_df, how='right', indicator=True)
    for_delete = diff_left[diff_left['_merge'] != 'both']
    to_add = diff_right[diff_right['_merge'] != 'both']

    for_delete = for_delete.drop(columns=['_merge'])
    to_add = to_add.drop(columns=['_merge'])

    return for_delete, to_add


def get_link_protocols_for_update(credential, count_last_protocol, name_point=[]):
    '''Функция возвращает список протоколов для сверки'''

    if len(name_point) != 0:
        result_list = [{'name_point': point} for point in name_point]
        condition = db.create_condition(result_list, 'OR')

        if count_last_protocol > 0:
            date_filter = f'''
                date_event IN (
                    SELECT DISTINCT date_event
                    FROM list_all_events
                    ORDER BY date_event DESC
                    LIMIT {count_last_protocol}
                )
            '''
        else:
            date_filter = 'TRUE'  # Без ограничения по датам

        request = f'''
SELECT *
FROM list_all_events
WHERE {date_filter}
  AND ({condition});
        '''
    else:
        if count_last_protocol > 0:
            request = f'''
SELECT *
FROM list_all_events
WHERE date_event IN (
    SELECT DISTINCT date_event
    FROM list_all_events
    ORDER BY date_event DESC
    LIMIT {count_last_protocol}
);
'''
        else:
            request = '''
SELECT *
FROM list_all_events;
'''

    engine = db.db_connect(credential)
    result = db.execute_request(engine, request)

    return result

def create_list_for_compare(credential):
    engine = db.db_connect(credential)
    request = 'SELECT name_point FROM general_location where is_pause = false'
    result = db.execute_request(engine, request)
    values_list = result.iloc[:, 0].tolist()
    return values_list

def record_or_update_protocol_by_link(credential: str, link: str):
    """
    По ссылке формата https://5verst.ru/<slug_парка>/results/DD.MM.YYYY/
    парсит протокол, определяет парк и дату, проверяет наличие в БД,
    вставляет недостающие данные или актуализирует расхождения.
    """

    # 1) Спарсить протокол → DF бегунов/волонтёров (как в твоём pipeline)
    final_df_run, final_df_vol = pp.main_parse(link)  # форматы под details_protocol / details_vol :contentReference[oaicite:0]{index=0}
    name_point = final_df_run["name_point"].iloc[0]
    date_event = final_df_run["date_event"].iloc[0]

    # 2) Получить строку для list_all_events из страницы «all results» парка
    main_link = link_handler.main_link_event(link)                            # базовая ссылка парка :contentReference[oaicite:1]{index=1}
    all_results_link = link_handler.link_all_result_event(main_link)          # .../results/all/ :contentReference[oaicite:2]{index=2}
    list_df_full = ptpp.transform_df_list_protocol(
        ptpp.list_protocols_in_park(all_results_link)
    )  # даёт столбцы под list_all_events (index_event, count_runners, ...) :contentReference[oaicite:3]{index=3}

    list_row = list_df_full[
        (list_df_full["name_point"] == name_point) &
        (list_df_full["date_event"] == pd.to_datetime(date_event))
    ].copy()

    # --- ВАЖНО: если в /results/all/ строки нет → прекращаем ---
    if list_row.empty:
        print("❌ Протокол не найден на странице всех результатов парка (/results/all/).")
        print("   Скорее всего ссылка указывает на тестовый/временный/неофициальный протокол,")
        print("   или на странице ещё не появилось обновление (ожидайте публикации).")
        print("   Запись/обновление в БД прервана.")
        return

    # 3) Проверить наличие протокола в БД по name_point + date_event
    engine = db.db_connect(credential)
    existing = db.get_inf_with_condition(
        engine, "list_all_events",
        [{"name_point": name_point}, {"date_event": date_event}]
    )  # выборка по ключу протокола :contentReference[oaicite:4]{index=4}

    if existing is None or existing.empty:
        # Новая запись
        for_removal_runner = pd.DataFrame(columns=["name_point","date_event","position"])
        for_removal_vol = pd.DataFrame(columns=["name_point","date_event","user_id","vol_role"])
        to_add_runner = final_df_run.copy()
        to_add_vol = (pd.DataFrame(columns=["name_point","date_event","name_runner","link_runner","user_id","vol_role"])
                      if final_df_vol is None else final_df_vol.copy())
        different_list_of_protocols = list_row.copy()  # чтобы через единый коммит записать строку list_all_events

        update_data_protocols(
            credential,
            for_removal_runner, for_removal_vol,
            to_add_runner, to_add_vol,
            different_list_of_protocols
        )  # транзакция + refresh вьюх (как у тебя принято) :contentReference[oaicite:5]{index=5}
        print(f"✅ Добавлен новый протокол: {name_point} — {pd.to_datetime(date_event).date()}")
        return

    # 4) Актуализация: сверить детали и строку list_all_events
    this_proto = pd.DataFrame([{"name_point": name_point, "date_event": date_event}])
    now_run, now_vol = get_now_protocols(credential, this_proto)              # выгрузка текущих деталей из БД :contentReference[oaicite:6]{index=6}

    for_removal_runner, to_add_runner = find_dif_protocol(final_df_run, now_run)  # дельты бегунов :contentReference[oaicite:7]{index=7}
    # учёт случая отсутствия блока волонтёров на сайте
    if final_df_vol is None:
        actual_vol = pd.DataFrame(columns=now_vol.columns) if not now_vol.empty else pd.DataFrame(
            columns=["name_point","date_event","name_runner","link_runner","user_id","vol_role"]
        )
    else:
        actual_vol = final_df_vol
    for_removal_vol, to_add_vol = find_dif_protocol(actual_vol, now_vol)          # дельты волонтёров :contentReference[oaicite:8]{index=8}

    # Проверить, отличается ли строка list_all_events (если да — заменим)
    need_replace_list = False
    cols_to_check = [c for c in list_row.columns if c in existing.columns and c != "updated_at"]
    if not list_row[cols_to_check].equals(existing[cols_to_check]):
        need_replace_list = True
        different_list_of_protocols = list_row.copy()
    else:
        different_list_of_protocols = pd.DataFrame()

    if (len(for_removal_runner) == 0 and len(to_add_runner) == 0 and
        len(for_removal_vol) == 0 and len(to_add_vol) == 0 and
        not need_replace_list):
        print("ℹ️ В БД уже актуальная информация по данному протоколу.")
        return

    update_data_protocols(
        credential,
        for_removal_runner, for_removal_vol,
        to_add_runner, to_add_vol,
        different_list_of_protocols
    )  # удалим лишнее / добавим недостающее / при необходимости заменим строку list_all_events :contentReference[oaicite:9]{index=9}

    print(f"✅ Обновлён протокол: {name_point} — {pd.to_datetime(date_event).date()}")
