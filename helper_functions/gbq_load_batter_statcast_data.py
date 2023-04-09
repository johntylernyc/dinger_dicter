# Purpose: Load the daily Statcast data into Google BigQuery    
# File Name: gbq_load_batter_statcast_data.py
# File Path: helper_functions/gbq_load_batter_statcast_data.py
# Author: John Tyler
# Date Created: 2023-4-08

# import the `bigquery` library
from google.cloud import bigquery

def load_batter_data_to_gbq(daily_statcast_data_csv):
    # define the bigquery project
    project_id = 'python-sandbox-381204'
    # define the name of the dataset
    dataset_name = 'dinger_dicter'
    # define the name of the table
    table_name = 'daily_statcast_data'

    # create a client object using the `python-sandbox-31204-1b0c0c5b5f8e.json` service account key in service_account_keys folder
    client = bigquery.Client.from_service_account_json('service_account_keys/python-sandbox-381204-18d99cdada13.json')

    # define the name of the dataset
    dataset_ref = client.dataset(dataset_name)
    # define the name of the table
    table_ref = dataset_ref.table(table_name)

    # check to see if the table already exists
    try:
        # try to load the table
        client.get_table(table_ref)
        # if the table already exists, delete it
        client.delete_table(table_ref)
        print('Table deleted.')
    except:
        # if the table does not exist, print a message
        print('Table does not exist.')

    # create a job config object
    job_config = bigquery.LoadJobConfig(
        # set the source format to CSV
        source_format=bigquery.SourceFormat.CSV,
        # set the write disposition to WRITE_TRUNCATE
        write_disposition='WRITE_TRUNCATE',
        # set the schema
        schema=[
            bigquery.SchemaField("game_date", "DATE"),
            bigquery.SchemaField("player_name", "STRING"),
            bigquery.SchemaField("launch_speed", "FLOAT64"),
            bigquery.SchemaField("launch_angle", "FLOAT64"),
            bigquery.SchemaField("pitch_type", "STRING"),
            bigquery.SchemaField("release_speed", "FLOAT64"),
            bigquery.SchemaField("p_throws", "STRING")
        ]
    )

    # define the path to the csv file
    csv_file_path = 'daily_statcast_data.csv'

    # open the csv file
    with open(csv_file_path, 'rb') as source_file:
        # load the csv file into the table
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        # wait for the job to complete
        job.result()
        # print a success message
        print('Table successfully created.')