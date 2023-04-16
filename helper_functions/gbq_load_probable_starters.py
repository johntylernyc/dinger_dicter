from .config_bigquery import project_id, dataset_name, probable_pitcher_table_name, json_key_path
from google.cloud import bigquery
import pandas as pd
import datetime

def load_probable_starters(df):
    client = bigquery.Client.from_service_account_json(json_key_path)
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(probable_pitcher_table_name)
    
    # add load date time column and convert time column to datetime
    def remove_timezone(time_str):
        return ' '.join(time_str.split()[:-1])
    
    def set_current_year(time_obj):
        current_year = datetime.datetime.now().year
        return time_obj.replace(year=current_year)

    # Assuming you have loaded the JSON data into a DataFrame called df
    df['time'] = df['time'].apply(remove_timezone)
    df['time'] = pd.to_datetime(df['time'], format='%a, %b %d • %I:%M %p')
    df['time'] = df['time'].apply(set_current_year)
    # set the load date time column to the current date and time
    df['load_date_time'] = datetime.datetime.now()

    # Configure the load job
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
    ]
    job_config.autodetect = True

    # Load the DataFrame to the table
    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    if load_job.errors:
        print("Error")
    else:
        print("Success")
        return load_job.job_id, len(df)

if __name__ == "__main__":
    probable_pitchers = pd.read_json('/Users/johntyler/Documents/GitHub/dinger_dicter/baseball-probable-pitchers/probable-pitchers-with-player-ids.json')
    load_probable_starters(probable_pitchers)