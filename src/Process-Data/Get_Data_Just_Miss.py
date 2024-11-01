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

# Función para obtener la última fecha almacenada en la base de datos
def obtener_ultima_fecha():
    # Obtener la última fecha almacenada en la base de datos
    cursor = con.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name DESC LIMIT 1;")
    result = cursor.fetchone()
    if result:
        return datetime.strptime(result[0], "%Y-%m-%d").date()
    return None

for key, value in config['get-data'].items():
    ultima_fecha = obtener_ultima_fecha()
    # Si ya tenemos datos, comenzamos desde el día siguiente de la última fecha
    date_pointer = ultima_fecha + timedelta(days=1) if ultima_fecha else datetime.strptime(value['start_date'], "%Y-%m-%d").date()
    end_date = datetime.strptime(value['end_date'], "%Y-%m-%d").date()

    while date_pointer <= end_date:
        print("Getting data: ", date_pointer)
        
        raw_data = get_json_data(
            url.format(date_pointer.month, date_pointer.day, value['start_year'], date_pointer.year, key))
        df = to_data_frame(raw_data)

        df['Date'] = str(date_pointer)
        
        # Guardar los datos en la base de datos
        df.to_sql(date_pointer.strftime("%Y-%m-%d"), con, if_exists="replace")
        
        date_pointer += timedelta(days=1)
        time.sleep(random.randint(1, 3))

con.close()
