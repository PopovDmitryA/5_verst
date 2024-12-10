import re

'''Извлечение user_id из ссылки на участника'''
def extract_user_id(link):
    try:
        return link.split('userstats/')[1]
    except IndexError:
        return None

'''Достаем ФИО из DF с протоколом пробежки, не обработанным'''
def trim_to_finish(string):  # Вычленяем ФИО
    finish_index = string.find("финиш") # Находим индекс слова "финиш"
    if finish_index != -1:  # Если "финиш" найден
        # Берём срез до "финиш"
        trimmed = string[:finish_index].rstrip()  # Убираем пробелы справа

        while trimmed and (trimmed[-1].isdigit() or trimmed[-1] == ' '): # Убираем пробелы и цифры в конце
            trimmed = trimmed[:-1]  # Убираем последний символ, если это пробел или цифра

        return trimmed.strip()  # Убираем лишние пробелы в конце
    return string  # Возвращаем исходную строку, если "финиш" не найден

'''Обрезаем по переданному word слову строку'''
def slice_until_word(string, word):
    index = string.find(word)
    return string[:index] if index != -1 else string

'''Выделение возрастной группы  регулярным выражением по знаку (
В будущем можно будет похожие действия унифицировать'''
def slice_before_parenthesis(value):
    if isinstance(value, str):  # Проверка, является ли значение строкой
        # Используем регулярное выражение для удаления пробела перед "("
        return re.split(r'\s*\(', value)[0].strip()  # Убираем пробелы в начале и конце
    return value  # Возвращаем None или другое значение без изменений

'''Записать в лог информацию о факте обновления той или иной таблицы в формате таблица-время'''
def time_to_log(engine, table, now_time):
    connection = engine.connect()
    insert_query = """
    INSERT INTO update_table (table_name, update_date)
    VALUES (%s, %s);
    """
    values_to_insert = (table, now_time) # Данные для вставки
    connection.execute(insert_query, values_to_insert) # Выполнение запроса

'''Парсинг страницы без ссылок. На выходе получаем dataframe'''
def parse_table(type_parse, link):

    if type_parse == 1: #Парсим голый линк, который подали на вход

    elif type_parse == 2  #Парсим страницу с результатами

    elif type_parse == 3 #Парсим протокол

    elif type_parse == 4 #Парсим таблицу волонтеров