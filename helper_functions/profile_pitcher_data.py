from google.cloud import bigquery 
from google.api_core.exceptions import NotFound
import pandas as pd
import datetime
from.config_bigquery import dataset_name, statcast_pitcher_table_name, json_key_path, project_id

def get_all_pitcher_data(): 
    client = bigquery.Client(project=project_id).from_service_account_json(json_key_path)
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(statcast_pitcher_table_name)
    table = client.get_table(table_ref)
    data = client.list_rows(table, max_results=1000000)
    df = data.to_dataframe()
    df = df.sort_values(by=['player_id', 'game_date'], ascending=True)
    return df


