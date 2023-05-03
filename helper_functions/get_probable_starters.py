import os
import subprocess
import json
from pybaseball import playerid_lookup
import pandas as pd
from google.cloud import bigquery


def create_probable_pitchers_table(project_id, dataset_name, table_name, json_key_path):
    client = bigquery.Client.from_service_account_json(json_key_path)
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)

    schema = [
        bigquery.SchemaField("teamAway", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("teamHome", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("time", "DATETIME", mode="REQUIRED"),
        bigquery.SchemaField("pitcherAway", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pitcherHome", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("away_player_key_mlbam", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("home_player_key_mlbam", "INT64", mode="REQUIRED")
    ]

    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)

    print(f"Created table {project_id}.{dataset_name}.{table_name}.")

#TODO: This is mostly working, but isn't adding doubleheaders to the database. 
def starters_exist_for_date(date, probable_pitchers, table_name, dataset_name, json_key_path, project_id):
    from google.cloud import bigquery
    from google.oauth2 import service_account
    from google.api_core.exceptions import NotFound


    credentials = service_account.Credentials.from_service_account_file(json_key_path)
    client = bigquery.Client(credentials=credentials, project=project_id)

    # Check if the table exists and create it if it doesn't
    try:
        client.get_table(f"{dataset_name}.{table_name}")
    except NotFound:
        create_probable_pitchers_table(project_id, dataset_name, table_name, json_key_path)

    query = f"""
        SELECT COUNT(*)
        FROM `{project_id}.{dataset_name}.{table_name}`
        WHERE DATE(`time`) = '{date}'
        AND away_player_key_mlbam = @away_mlbam
        AND home_player_key_mlbam = @home_mlbam
    """

    # Iterate over the games in the probable_pitchers DataFrame
    duplicate_count = 0
    for index, row in probable_pitchers.iterrows():
        away_mlbam = row['away_player_key_mlbam']
        home_mlbam = row['home_player_key_mlbam']
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("away_mlbam", "INT64", away_mlbam),
                bigquery.ScalarQueryParameter("home_mlbam", "INT64", home_mlbam),
            ]
        )

        query_job = client.query(query, job_config=job_config)
        result = query_job.result()
        count = list(result)[0][0]
        duplicate_count += count

    return duplicate_count

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

    os.remove(probable_pitchers_file)
    probable_pitchers = pd.DataFrame(probable_pitchers_data)
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