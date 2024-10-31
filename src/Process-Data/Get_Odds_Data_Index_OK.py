import os
import random
import sqlite3
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import toml
from sbrscrape import Scoreboard

# TODO: Add tests

sys.path.insert(1, os.path.join(sys.path[0], '../..'))

sportsbook = 'fanduel'
df_data = []

# Cargar la configuración
config = toml.load("C:/Python310/nba/NBA-Machine-Learning-Sports-Betting/config.toml")

# Conectar a la base de datos SQLite
con = sqlite3.connect("../../Data/OddsData.sqlite")

for key, value in config['get-odds-data'].items():
    date_pointer = datetime.strptime(value['start_date'], "%Y-%m-%d").date()
    end_date = datetime.strptime(value['end_date'], "%Y-%m-%d").date()
    teams_last_played = {}

    while date_pointer <= end_date:
        print("Getting odds data: ", date_pointer)
        sb = Scoreboard(date=date_pointer)

        if not hasattr(sb, "games"):
            date_pointer += timedelta(days=1)
            continue

        for game in sb.games:
            if game['home_team'] not in teams_last_played:
                teams_last_played[game['home_team']] = date_pointer
                home_games_rested = timedelta(days=7)  # start of season, big number
            else:
                current_date = date_pointer
                home_games_rested = current_date - teams_last_played[game['home_team']]
                teams_last_played[game['home_team']] = current_date

            if game['away_team'] not in teams_last_played:
                teams_last_played[game['away_team']] = date_pointer
                away_games_rested = timedelta(days=7)  # start of season, big number
            else:
                current_date = date_pointer
                away_games_rested = current_date - teams_last_played[game['away_team']]
                teams_last_played[game['away_team']] = current_date

            try:
                df_data.append({
                    'Date': date_pointer,
                    'Home': game['home_team'],
                    'Away': game['away_team'],
                    'OU': game['total'][sportsbook],
                    'Spread': game['away_spread'][sportsbook],
                    'ML_Home': game['home_ml'][sportsbook],
                    'ML_Away': game['away_ml'][sportsbook],
                    'Points': game['away_score'] + game['home_score'],
                    'Win_Margin': game['home_score'] - game['away_score'],
                    'Days_Rest_Home': home_games_rested.days,
                    'Days_Rest_Away': away_games_rested.days
                })
            except KeyError:
                print(f"No {sportsbook} odds data found for game: {game}")

        date_pointer += timedelta(days=1)
        time.sleep(random.randint(1, 3))

    # Convertir los datos a DataFrame
    df = pd.DataFrame(df_data)

    # Generar un nombre único para la tabla para evitar conflictos de índice
    table_name = f"{key}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Guardar en la base de datos
    df.to_sql(table_name, con, if_exists="replace", index=False)
    print(f"Data stored in table {table_name}")

con.close()
