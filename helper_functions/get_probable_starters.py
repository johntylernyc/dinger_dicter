import os
import subprocess
import json
import re
from datetime import datetime
from pybaseball import playerid_lookup
import pandas as pd
from dateutil import parser
from google.cloud import bigquery


def create_probable_pitchers_table(project_id, dataset_name, table_name, json_key_path):
    client = bigquery.Client.from_service_account_json(json_key_path)
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)

    schema = [
        bigquery.SchemaField("away_team", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("home_team", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("game_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("game_time", "DATETIME", mode="REQUIRED"),
        bigquery.SchemaField("away_pitcher", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("home_pitcher", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("away_player_key_mlbam", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("home_player_key_mlbam", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("load_date_time", "DATETIME", mode="REQUIRED")
    ]

    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)

    print(f"Created table {project_id}.{dataset_name}.{table_name}.")

def run_npm_start(date):
    try:
        project_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'baseball-probable-pitchers')
        process = subprocess.Popen(["npm", "start"], cwd=project_path, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=dict(os.environ, DATE_TO_FETCH=date))
        stdout, stderr = process.communicate()
        print(f"Success: {stdout.decode('utf-8')}") if process.returncode == 0 else print(f"Error: {stderr.decode('utf-8')}")
    except Exception as e:
        print(f"An error occurred: {e}")

def get_probable_starters(date):
    run_npm_start(date)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_path = os.path.join(script_dir, 'baseball-probable-pitchers')
    probable_pitchers_file = os.path.join(project_path, 'probable-pitchers.json')

    with open(probable_pitchers_file, 'r') as f:
        json_data = json.load(f)
        probable_pitchers_data = json_data['matchups']

    probable_pitchers = pd.DataFrame(probable_pitchers_data)

    def clean_time(time_str):
        if 'Bot' in time_str or 'Top' in time_str or 'final' in time_str:
            return datetime.now().replace(hour=4, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M')
        else:
            # Remove timezone
            time_str = re.sub(r'\s\w+$', '', time_str)
            # Replace unusual characters and strip unnecessary parts
            time_str = re.sub(r'[^a-zA-Z0-9:\s]', '', re.sub(r'^\w+, ', '', time_str))
            # Parse the date
            dt = parser.parse(time_str)
            # Return it in the desired format
            return dt.strftime('%Y-%m-%d %H:%M')

    probable_pitchers['time'] = probable_pitchers['time'].apply(clean_time)

    away_player_mlbam = []
    home_player_mlbam = []

    def get_player_mlbam(player_name):
        # Check if player_name is 'TBD' or does not contain a space
        if player_name == 'TBD' or ' ' not in player_name:
            # In this case, you can return None or handle it differently if needed
            return None
        # If player_name is not 'TBD' and has a space, split it into first_name and last_name
        first_name, last_name = player_name.split(' ')
        player_mlbam = playerid_lookup(last_name, first_name, fuzzy=True)
        return int(player_mlbam['key_mlbam'].values[0]) if not player_mlbam.empty else None

    for index, row in probable_pitchers.iterrows():
        away_player_mlbam.append(get_player_mlbam(row['pitcherAway']))
        home_player_mlbam.append(get_player_mlbam(row['pitcherHome']))

    probable_pitchers['away_player_key_mlbam'] = away_player_mlbam
    probable_pitchers['home_player_key_mlbam'] = home_player_mlbam

    probable_pitchers_with_player_ids_file = os.path.join(project_path, 'probable-pitchers-with-player-ids.json')
    probable_pitchers.to_json(probable_pitchers_with_player_ids_file, orient='records')

    return probable_pitchers

if __name__ == "__main__":
    run_npm_start()
    probable_pitchers = get_probable_starters()
    print(probable_pitchers) 