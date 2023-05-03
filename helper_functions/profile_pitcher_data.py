from google.cloud import bigquery 
from google.api_core.exceptions import NotFound
import pandas as pd
import datetime
from.config_bigquery import dataset_name, statcast_pitcher_table_name, json_key_path, project_id
from google.cloud.bigquery import SchemaField


def get_all_pitcher_data():
    # create a client object using the `python-sandbox-31204-1b0c0c5b5f8e.json` service account key in service_account_keys folder. 
    client = bigquery.Client(project=project_id).from_service_account_json(json_key_path)
    # define the name of the dataset
    dataset_ref = client.dataset(dataset_name)
    # define the name of the table
    table_ref = dataset_ref.table(statcast_pitcher_table_name)
    #load the table
    table = client.get_table(table_ref)
    # get the data from the table
    data = client.list_rows(table, max_results=2000000)
    # store the data in a pandas dataframe
    df = data.to_dataframe()
    # sort the dataframe by player_id and game_date in descending order
    df = df.sort_values(by=['player_id', 'game_date'], ascending=True)
    # return the pandas dataframe
    return df

def preprocess_pitcher_data(df):
    # remove all rows where game_date is null
    df = df[df['game_date'].notna()]
    # remove all rows where player_name is null
    df = df[df['player_name'].notna()]
    # remove all rows where player_id is null
    df = df[df['player_id'].notna()]
    # if launch_speed is null, set it to the median for that player_id
    df['release_speed'] = df.groupby('player_id')['release_speed'].apply(lambda x: x.fillna(x.median()))
    # if launch_angle is null, set it to the median for that player_id
    df['release_spin_rate'] = df.groupby('player_id')['release_spin_rate'].apply(lambda x: x.fillna(x.median()))
    # if the pitch_type is null, drop the row
    df = df[df['pitch_type'].notna()]
    # if the p_throws is null, drop the row
    df = df[df['p_throws'].notna()]
    # if the spin_rate is null, drop the row
    return df

def add_pitcher_features(df):
    print(f"Number of rows after preprocessing: {len(df)}")
    df['release_spin_rate_bin'] = pd.qcut(df['release_spin_rate'], q=3, labels=['low', 'medium', 'high'])
    df['game_month'] = pd.DatetimeIndex(df['game_date']).month
    df['release_speed_bin'] = pd.qcut(df['release_speed'], q=3, labels=['low', 'medium', 'high'])
    df['pitch_type_p_throws'] = df['pitch_type'] + '_' + df['p_throws']
    df['game_date'] = pd.to_datetime(df['game_date'])
    df['game_date'] = df['game_date'].dt.date
    return df 

def create_new_pitcher_df_with_features_and_tests():
    def data_quality_check(df, column_name):
        if df[column_name].isnull().sum() == 0:
            print(f"{column_name} data quality check complete")
        else:
            print(f"{column_name} data quality check failed")

    df = add_pitcher_features(preprocess_pitcher_data(get_all_pitcher_data()))

    columns_to_check = [
        "player_name", "player_id", "game_date", "pitch_type", "p_throws",
        "game_month", "release_speed_bin", "pitch_type_p_throws", 
        "release_spin_rate_bin", "release_spin_rate"
    ]

    for column in columns_to_check:
        data_quality_check(df, column)

    print('All tests passed!')
    return df

def load_pitcher_profile_data(df):
    # create a BigQuery client
    client = bigquery.Client(project=project_id).from_service_account_json(json_key_path)
    # create a BigQuery dataset reference
    dataset_ref = client.dataset(dataset_name)
    # create a BigQuery table reference
    table_ref = dataset_ref.table(statcast_pitcher_table_name + '_with_features')
    # initialize the job variable
    job_result = None
    # define the schema
    schema = [
        bigquery.SchemaField("player_name", "STRING", mode="NULLABLE", description="Name of the player"),
        bigquery.SchemaField("player_id", "STRING", mode="NULLABLE", description="ID of the player"),
        bigquery.SchemaField("game_date", "DATE", mode="NULLABLE", description="Date of the game"),
        bigquery.SchemaField("pitch_type", "STRING", mode="NULLABLE", description="Type of pitch thrown"),
        bigquery.SchemaField("p_throws", "STRING", mode="NULLABLE", description="Pitcher's throwing hand"),
        bigquery.SchemaField("game_month", "INTEGER", mode="NULLABLE", description="Month of the game"),
        bigquery.SchemaField("release_speed_bin", "STRING", mode="NULLABLE", description="Binned release speed"),
        bigquery.SchemaField("pitch_type_p_throws", "STRING", mode="NULLABLE", description="Combined pitch type and throwing hand"),
        bigquery.SchemaField("release_spin_rate_bin", "STRING", mode="NULLABLE", description="Binned release spin rate"),
        bigquery.SchemaField("release_spin_rate", "FLOAT", mode="NULLABLE", description="Release spin rate of the pitch"),
        bigquery.SchemaField("date_added", "DATETIME", mode="NULLABLE", description="Date the data was added to the table")
    ]

        # Try to get the table, if NotFound exception is raised, create the table
    try:
        table = client.get_table(table_ref)
    except NotFound:
        # create the table
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print('Created table {} in dataset {}.'.format(table.table_id, dataset_name))
    # load the data into the table
    if table.num_rows == 0:
        # create a dataframe and add a column for the date the data is being added, set as today's date
        df['date_added'] = datetime.date.today()
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.autodetect = True
        job_config.schema = schema
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job_result = job.result()
        print('Loaded {} rows into {}:{}.'.format(job_result.output_rows, dataset_name, statcast_pitcher_table_name + '_with_features'))
        return job_result.job_id, job_result.output_rows
    # if there is data in the table, only load the data that is not already in the table
    else:
        # create a dataframe with only the data that is not already in the table
        table = client.get_table(table_ref)
        table_dataframe = client.list_rows(table).to_dataframe()
        new_df = df[~df['game_date'].isin(table_dataframe['game_date'])]
        new_df['date_added'] = datetime.date.today()
        # load the new data into BigQuery
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.autodetect = True
        job_config.schema = schema
        job = client.load_table_from_dataframe(new_df, table_ref, job_config=job_config)
        job_result = job.result()
        print('Loaded {} rows into {}:{}.'.format(job_result.output_rows, dataset_name, statcast_pitcher_table_name + '_with_features'))
        return job_result.job_id

def create_pitcher_summary_dataframe(df):
    # Group by player_id and get summary statistics
    player_summary = df.groupby('player_id').agg({
        'player_name': 'first',
        'release_spin_rate': ['mean','std'],
        'release_speed': ['mean','std'],
    })

    # Flatten multi-level columns
    player_summary.columns = ['_'.join(col).strip() for col in player_summary.columns.values]

    # Fix Column Names
    player_summary.rename(columns={'player_name_first': 'player_name'}, inplace=True)

    # Calculate pitch type proportions
    pitch_type_proportions = df.pivot_table(index='player_id', columns=['p_throws', 'pitch_type'], values=None, aggfunc='size', fill_value=0)
    pitch_type_proportions = pitch_type_proportions.div(pitch_type_proportions.sum(axis=1), axis=0)
    pitch_type_proportions.columns = ['proportion_of_pitch_type_' + '_'.join(col) for col in pitch_type_proportions.columns.values]

    # Calculate pitch type proportions regardless of p_throws
    pitch_type_proportions_all = df.pivot_table(index='player_id', columns='pitch_type', values=None, aggfunc='size', fill_value=0)
    pitch_type_proportions_all = pitch_type_proportions_all.div(pitch_type_proportions_all.sum(axis=1), axis=0)
    pitch_type_proportions_all.columns = ['proportion_of_pitch_type_' + col for col in pitch_type_proportions_all.columns.values]

    # Join the player_summary and pitch_type_proportions dataframes
    summary_df = player_summary.join(pitch_type_proportions)
    # Join the player_summary and pitch_type_proportions_all dataframes
    summary_df = summary_df.join(pitch_type_proportions_all)

    return summary_df.reset_index()

def load_pitcher_summary_data(df):
    # create a BigQuery client
    client = bigquery.Client(project=project_id).from_service_account_json(json_key_path)
    # create a dataset reference
    dataset_ref = client.dataset(dataset_name)
    # create a table reference
    table_ref = dataset_ref.table(statcast_pitcher_table_name + '_summary')
    # initialize the job variable
    job_result = None
    def generate_bigquery_schema(df):
        schema = []

        for column in df.columns:
            if column == 'player_id':
                field_type = 'INT64'
            elif column == 'player_name':
                field_type = 'STRING'
            elif column == 'date_added':  # Add this condition to handle 'date_added' column type
                field_type = 'DATE'  # Replace 'DATE' with 'DATETIME' if it is a datetime column
            else:
                field_type = 'FLOAT'

            # Keep schema.append() inside the for loop but at the same level of indentation as the if-elif-else statements
            schema.append(
                bigquery.SchemaField(column, field_type, mode='REQUIRED', description='Description of {}'.format(column))
            )

        return schema
    
    bq_schema = generate_bigquery_schema(df)

    # Verify if the DataFrame and schema have the same columns
    df_columns = set(df.columns)
    schema_columns = {field.name for field in bq_schema}

    if df_columns != schema_columns:
        print("Mismatch between DataFrame columns and schema columns:")
        print("Columns in DataFrame but not in schema:", df_columns - schema_columns)
        print("Columns in schema but not in DataFrame:", schema_columns - df_columns)
    else:
        print("DataFrame and schema have the same columns.")

    # Try to get the table, if NotFound exception is raised, create the table
    try:
        table = client.get_table(table_ref)
    except NotFound:
        # create the table
        table = bigquery.Table(table_ref, schema=bq_schema)
        table = client.create_table(table)
        print('Created table {}.{}'.format(table.table_id, dataset_name))
    if table.num_rows == 0:
        # create a dataframe and add a column for the date the data is being added, set as today's date
        df['date_added'] = datetime.date.today()
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.autodetect = True
        job_config.schema = bq_schema
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job_result = job.result()
        print('Loaded {} rows into {}:{}.'.format(job_result.output_rows, dataset_name, statcast_pitcher_table_name + '_summary'))
        return job_result.job_id, job_result.output_rows
    # if there is data in the table, only load the data that is not already in the table. If date_added < today, replace the row with the new data.
    else:
        # get the date_added column from the table
        query_job = client.query(
            """
            SELECT date_added
            FROM `{}.{}.{}` 
            """.format(project_id, dataset_name, statcast_pitcher_table_name + '_summary')
        )
        results = query_job.result()
        date_added = []
        for row in results:
            date_added.append(row.date_added)
        # create a dataframe and add a column for the date the data is being added, set as today's date
        df['date_added'] = datetime.date.today()
        # create a dataframe with the data that is not in the table
        df = df[~df['date_added'].isin(date_added)]
        # if there is data that is not in the table, load the data into the table
        if len(df) > 0:
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            job_config.source_format = bigquery.SourceFormat.CSV
            job_config.skip_leading_rows = 1
            job_config.autodetect = True
            job_config.schema = bq_schema
            job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job_result = job.result()
            print('Loaded {} rows into {}:{}.'.format(job_result.output_rows, dataset_name, statcast_pitcher_table_name + '_summary'))
            return job_result.job_id, job_result.output_rows
        else:
            print('No new data to load into {}:{}.'.format(dataset_name, statcast_pitcher_table_name + '_summary'))
            return None, None