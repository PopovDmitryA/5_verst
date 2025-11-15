import configparser
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from geopy.distance import geodesic

# Функция для расчёта дистанции
def add_distance_loc(latitude, longitude):
    coordinate_cremlin = (55.7522, 37.6156)
    new_point = (latitude, longitude)
    distance_km = geodesic(coordinate_cremlin, new_point).kilometers
    return distance_km

# Чтение конфигурации
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
CONFIG_PATH = PROJECT_ROOT / "5_verst.ini"

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

db_host = config['five_verst_stats']['host']
db_user = config['five_verst_stats']['username']
db_pass = config['five_verst_stats']['password']
db_name = config['five_verst_stats']['dbname']

# Подключение к базе через SQLAlchemy
credential = f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}'
engine = create_engine(credential)

print("Подключение к базе выполнено.")

# Считываем только строки, где distance_from_cremlin IS NULL
with engine.connect() as conn:
    df = pd.read_sql("""
        SELECT name_point, latitude, longitude 
        FROM s95_location 
        WHERE distance_from_cremlin IS NULL
    """, conn)

print(f"Считано {len(df)} строк с пустым distance_from_cremlin.")

# Вычисляем расстояние только для строк с координатами
df['distance_from_cremlin'] = df.apply(
    lambda row: add_distance_loc(row['latitude'], row['longitude'])
    if pd.notnull(row['latitude']) and pd.notnull(row['longitude']) else None,
    axis=1
)

# Фильтруем строки, которые нужно обновить
to_update_df = df[df['distance_from_cremlin'].notnull()]
print(f"Будет обновлено {len(to_update_df)} строк.")

# Обновляем таблицу в транзакции с автоматическим коммитом
with engine.begin() as conn:
    for idx, row in to_update_df.iterrows():
        conn.execute(
            text("""
                UPDATE s95_location
                SET distance_from_cremlin = :distance
                WHERE name_point = :name_point
            """),
            {"distance": row['distance_from_cremlin'], "name_point": row['name_point']}
        )
        print(f"Обновлена точка {row['name_point']}, distance = {row['distance_from_cremlin']:.2f} км")
