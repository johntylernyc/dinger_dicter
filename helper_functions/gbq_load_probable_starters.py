from .config_bigquery import project_id, dataset_name, probable_pitcher_table_name, json_key_path
from google.cloud import bigquery
import pandas as pd
import datetime
from google.api_core.exceptions import NotFound


def load_probable_starters(df, table_name, json_key_path):
    client = bigquery.Client.from_service_account_json(json_key_path)
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)

    # Rename the columns
    df.rename(columns={'teamAway': 'away_team',
                       'teamHome': 'home_team',
                       'time': 'game_time',
                       'pitcherAway': 'away_pitcher',
                       'pitcherHome': 'home_pitcher'}, inplace=True)

    # Set the load date time column to the current date and time
    df['load_date_time'] = datetime.datetime.now()
    df['load_date_time'] = df['load_date_time'].dt.strftime('%Y-%m-%d %H:%M')
    df['load_date_time'] = pd.to_datetime(df['load_date_time'])

    # Convert columns to nullable integer type
    df['away_player_key_mlbam'] = df['away_player_key_mlbam'].astype(object)
    df['home_player_key_mlbam'] = df['home_player_key_mlbam'].astype(object)

    # Set the game_date column to the date of the game
    df['game_time'] = pd.to_datetime(df['game_time'])
    df['game_time'] = df['game_time'].apply(lambda x: x.to_pydatetime())
    df['game_date'] = df['game_time'].dt.date

    # Define the full table ID
    full_table_id = f"{dataset_name}.{table_name}"

    # # attempt to resolve str to int error
    # df['away_player_key_mlbam'] = df['away_player_key_mlbam'].astype(int)
    # df['home_player_key_mlbam'] = df['home_player_key_mlbam'].astype(int)
    # df['load_date_time'] = pd.to_datetime(df['load_date_time'])
    # # user game_time column to set the game_date column to the date of the game
    # df['game_date'] = df['game_time'].dt.date

    # Define the full table ID
    full_table_id = f"{project_id}.{dataset_name}.{table_name}"

    # Check if the table exists
    try:
        client.get_table(full_table_id)
    except NotFound:
        print(f"Table {full_table_id} not found. Creating and loading the data.")
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
        job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
        return  # Exit the function after loading the data into the new table

    # Load your DataFrame into a temporary table
    table_id_temp = f"{project_id}.{dataset_name}.tmp_probable_pitchers"
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job = client.load_table_from_dataframe(df, table_id_temp, job_config=job_config)
    job.result()  # Wait for the job to complete

    # Define your SQL query
    sql = f"""
    MERGE `{project_id}.{dataset_name}.probable_pitchers_daily` AS T
    USING `{project_id}.{dataset_name}.tmp_probable_pitchers` AS S
    ON T.away_team = S.away_team AND T.home_team = S.home_team AND T.game_time = S.game_time
    WHEN NOT MATCHED THEN
    INSERT (away_team, home_team, game_time, away_pitcher, home_pitcher, away_player_key_mlbam, home_player_key_mlbam, load_date_time, game_date)
    VALUES (away_team, home_team, game_time, away_pitcher, home_pitcher, away_player_key_mlbam, home_player_key_mlbam, load_date_time, game_date)
    """
    # Execute the SQL query
    client.query(sql).result()

    # Clean up the temporary table
    client.delete_table(table_id_temp)

if __name__ == "__main__":
    probable_pitchers = pd.read_json(
        '/Users/johntyler/Documents/GitHub/dinger_dicter/helper_functions/baseball-probable-pitchers/probable-pitchers-with-player-ids.json')

    from .config_bigquery import project_id, dataset_name, probable_pitcher_table_name, json_key_path

    load_probable_starters(probable_pitchers, probable_pitcher_table_name, json_key_path, project_id)
