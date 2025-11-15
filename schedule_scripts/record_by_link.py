import update_data_functions as udf
from .update_data_main import credential
from datetime import datetime

def record_by_link():
    """
    Запрашивает ссылку на протокол и вызывает запись/актуализацию.
    Пример ссылки: https://5verst.ru/parkbondina/results/01.11.2025/
    """
    print(f'{datetime.now()}: Запуск записи/актуализации протокола по ссылке')
    link = input("Вставьте ссылку вида https://5verst.ru/<slug>/results/DD.MM.YYYY/: ").strip()
    if not link:
        print("Пустая ссылка. Операция отменена.")
        return

    udf.record_or_update_protocol_by_link(credential, link)

if __name__ == "__main__":
    record_by_link()
