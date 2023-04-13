from google.cloud import bigquery

def load_batter_data_to_gbq(daily_statcast_data_csv, dataset_name, statcast_batter_table_name, json_key_path, project_id):
    # create a client object using the `python-sandbox-31204-1b0c0c5b5f8e.json` service account key in service_account_keys folder
    client = bigquery.Client(project=project_id).from_service_account_json(json_key_path)
    # define the name of the dataset
    dataset_ref = client.dataset(dataset_name)
    # define the name of the table
    table_ref = dataset_ref.table(statcast_batter_table_name)

    # check to see if the table already exists
    try:
        # try to load the table
        client.get_table(table_ref)
        # if the table already exists print a message
        print('Table already exists.')
        # TODO: Currently this just appends the data instead of checking to see if the data already exists. Update to check whether the data already exists.
        # append the csv data to the existing table
        job_config = bigquery.LoadJobConfig(
            # set the source format to CSV
            source_format=bigquery.SourceFormat.CSV,
            # set the write disposition to WRITE_APPEND
            write_disposition='WRITE_APPEND',
            # set the schema
            schema=[
                bigquery.SchemaField("player_id", "INT64", description="The unique ID of the player"),
                bigquery.SchemaField("game_date", "DATE", description="The date of the game."),
                bigquery.SchemaField("player_name", "STRING", description="The name of the player."),
                bigquery.SchemaField("launch_speed", "FLOAT64", description="The speed of the ball when it leaves the bat."),
                bigquery.SchemaField("launch_angle", "FLOAT64", description="The angle of the ball when it leaves the bat."),
                bigquery.SchemaField("pitch_type", "STRING", description="The type of pitch thrown."),
                bigquery.SchemaField("release_speed", "FLOAT64", description="The speed of the pitch when it leaves the pitcher's hand."),
                bigquery.SchemaField("p_throws", "STRING", description="The hand of the pitcher. (L or R)")
            ]
        )

        # open the csv file
        with open(daily_statcast_data_csv, 'rb') as source_file:
            # load the csv file into the table
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
            # wait for the job to complete
            job.result()
            # print a message
            print('Loaded {} rows into {}:{}.'.format(job.output_rows, dataset_name, statcast_batter_table_name))

    # if the table does not exist, create it
    except:
        # print a message
        print('Table does not exist.')
        # create a job config object
        job_config = bigquery.LoadJobConfig(
            # set the source format to CSV
            source_format=bigquery.SourceFormat.CSV,
            # set the write disposition to WRITE_TRUNCATE
            write_disposition='WRITE_TRUNCATE',
            # set the schema add column definitions
            schema=[
                bigquery.SchemaField("player_id", "INT64", description="The unique ID of the player"),
                bigquery.SchemaField("game_date", "DATE", description="The date of the game."),
                bigquery.SchemaField("player_name", "STRING", description="The name of the player."),
                bigquery.SchemaField("launch_speed", "FLOAT64", description="The speed of the ball when it leaves the bat."),
                bigquery.SchemaField("launch_angle", "FLOAT64", description="The angle of the ball when it leaves the bat."),
                bigquery.SchemaField("pitch_type", "STRING", description="The type of pitch thrown."),
                bigquery.SchemaField("release_speed", "FLOAT64", description="The speed of the pitch when it leaves the pitcher's hand."),
                bigquery.SchemaField("p_throws", "STRING", description="The hand of the pitcher.")
            ]
        )

        # open the csv file
        with open(daily_statcast_data_csv, 'rb') as source_file:
            # load the csv file into the table
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
            # wait for the job to complete
            job.result()
            # print a success message
            print('Table successfully created.')
            # print a message
            print('Loaded {} rows into {}:{}.'.format(job.output_rows, dataset_name, statcast_batter_table_name))

# # test the function
# if __name__ == '__main__':
#     # define the path to the csv file
#     daily_statcast_data_csv = '/Users/johntyler/Documents/GitHub/dinger_dicter/daily_statcast_data.csv'
#     # define the project id
#     project_id = 'python-sandbox-381204'
#     # define the dataset name
#     dataset_name = 'dinger_dicter'
#     # define the table name
#     statcast_batter_table_name = 'homerun_batter_statcast_data'
#     # define the path to the service account key
#     json_key_path = '/Users/johntyler/Documents/GitHub/dinger_dicter/service_account_keys/python-sandbox-381204-18d99cdada13.json'
#     # call the function
#     load_batter_data_to_gbq(daily_statcast_data_csv, dataset_name, statcast_batter_table_name, json_key_path)