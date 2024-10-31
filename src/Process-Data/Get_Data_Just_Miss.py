import os
import random
import sqlite3
import sys
import time
from datetime import datetime, timedelta

import toml

sys.path.insert(1, os.path.join(sys.path[0], '../..'))
from src.Utils.tools import get_json_data, to_data_frame

config = toml.load("../../config.toml")

url = config['data_url']

con = sqlite3.connect("../../Data/TeamData.sqlite")
cursor = con.cursor()

for key, value in config['get-data'].items():
    start_date = datetime.strptime(value['start_date'], "%Y-%m-%d").date()
    end_date = datetime.strptime(value['end_date'], "%Y-%m-%d").date()
    
    # Verificar la última fecha existente para esta tabla en la base de datos
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '{key}_%'")
    existing_tables = cursor.fetchall()
    last_date = start_date  # Valor predeterminado en caso de que no haya tablas previas
    
    if existing_tables:
        # Extraer la última fecha de los nombres de las tablas
        last_dates = [datetime.strptime(table[0].split('_')[1], "%Y-%m-%d").date() for table in existing_tables]
        last_date = max(last_dates)
    
    # Ajustar el date_pointer para comenzar desde el día después de la última fecha registrada
    date_pointer = last_date + timedelta(days=1)

    while date_pointer <= end_date:
        print("Getting data: ", date_pointer)

        raw_data = get_json_data(
            url.format(date_pointer.month, date_pointer.day, value['start_year'], date_pointer.year, key))
        df = to_data_frame(raw_data)

        df['Date'] = str(date_pointer)
        df.to_sql(f"{key}_{date_pointer.strftime('%Y-%m-%d')}", con, if_exists="replace")

        date_pointer += timedelta(days=1)
        time.sleep(random.randint(1, 3))

con.close()
