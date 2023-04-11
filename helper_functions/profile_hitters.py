# File Name: profile_hitters.py
# File Path: helper_functions/profile_hitters.py
# Author: John Tyler
# Created: 2023-04-10

from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import pandas as pd
import datetime
from config import dataset_name, statcast_batter_table_name, json_key_path, project_id
import os 

# set the environment variable for the GCP project 
os.environ['GOOGLE_CLOUD_PROJECT'] = project_id
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_key_path

'''
Write a function that gets all available data from BigQuery using the config.py file variables. 
Store the data in a pandas dataframe.
'''

def get_all_data():
    # create a client object using the `python-sandbox-31204-1b0c0c5b5f8e.json` service account key in service_account_keys folder. 
    client = bigquery.Client.from_service_account_json(json_key_path)
    # define the name of the dataset
    dataset_ref = client.dataset(dataset_name)
    # define the name of the table
    table_ref = dataset_ref.table(statcast_batter_table_name)
    #load the table
    table = client.get_table(table_ref)
    # get the data from the table
    data = client.list_rows(table, max_results=1000000)
    # store the data in a pandas dataframe
    df = data.to_dataframe()
    # sort the dataframe by player_id and game_date in descending order
    df = df.sort_values(by=['player_id', 'game_date'], ascending=True)
    # return the pandas dataframe
    return df

''' 
Write a function to preprocess the data in the dataframe, specifically looking for the following: 
1. Remove all rows where player_name is null. 
2. Remove all rows where the player_id is null.
3. If launch_speed is null, set it to the median for that player_id. 
4. If launch_angle is null, set it to the median for that player_id.
5. If the pitch_type is null, drop the row. 
6. If the release_speed is null, set it to the median for that pitch_type for the given player_id.
7. If the p_throws is null, drop the row.
'''

def preprocess_data(df):
    # remove all rows where game_date is null
    df = df[df['game_date'].notna()]
    # remove all rows where player_name is null
    df = df[df['player_name'].notna()]
    # remove all rows where player_id is null
    df = df[df['player_id'].notna()]
    # if launch_speed is null, set it to the median for that player_id
    df['launch_speed'] = df.groupby('player_id')['launch_speed'].apply(lambda x: x.fillna(x.median()))
    # if launch_angle is null, set it to the median for that player_id
    df['launch_angle'] = df.groupby('player_id')['launch_angle'].apply(lambda x: x.fillna(x.median()))
    # if the pitch_type is null, drop the row
    df = df[df['pitch_type'].notna()]
    # if the release_speed is null, set it to the median for that pitch_type for the given player_id
    df['release_speed'] = df.groupby(['player_id', 'pitch_type'])['release_speed'].apply(lambda x: x.fillna(x.median()))
    # if the p_throws is null, drop the row
    df = df[df['p_throws'].notna()]
    # return the dataframe
    return df

'''
Write a function to add features to the home runs hit for each of the hitters in the dataframe. For each row, calculate the following:
1. Bin the launch_speed into 3 bins: weak, normal, hard. These should use the 33rd, 50th, and 67th percentiles across the entire dataset, respectively.
2. Bin the launch_angle into 3 bins: low, medium, high. These should use the 33rd, 50th, and 67th percentiles across the entire dataset, respectively.
3. Calculate the difference between the launch_speed and the release_speed.
4. Calculate the difference between the launch_angle and the release_speed.
5. Calculate the game month as a number from 1-12 using the game_date column.
6. Calculate the number of days since the player's last home run, using the game_date column and the player_id column.
7. Calculate the cumulative number of home runs across all games for the player up to that point, using the player_id column.
8. Calculate the moving average for the launch_speed for the player up to that point for the last 20 home runs, using the game_date and player_id columns.
9. Calculate the moving average for the launch_angle for the player up to that point for the last 20 home runs, using the game_date player_id columns.
10. Bin the release_speed into 3 bins: slow, normal, fast. These should use the 33rd, 50th, and 67th percentiles across the entire dataset, respectively.
11. Create a new feature that combines the "pitch_type" and "p_throws" columns. For example, if the pitch_type is "FF" and the p_throws is "R", the new feature should be "FF_R". If the pitch_type is "FF" and the p_throws is "L", the new feature should be "FF_L".
'''

def add_features(df):
    # 1. bin the launch_speed into 3 bins: weak, normal, hard. These should use the 33rd, 50th, and 67th percentiles across the entire dataset, respectively.
    df['launch_speed_bin'] = pd.qcut(df['launch_speed'], q=3, labels=['weak', 'normal', 'hard'])
    # 2. bin the launch_angle into 3 bins: low, medium, high. These should use the 33rd, 50th, and 67th percentiles across the entire dataset, respectively.
    df['launch_angle_bin'] = pd.qcut(df['launch_angle'], q=3, labels=['low', 'medium', 'high'])
    # 3. calculate the difference between the launch_speed and the release_speed
    df['speed_diff'] = df['launch_speed'] - df['release_speed']
    # 4. calculate the difference between the launch_angle and the release_speed
    df['angle_diff'] = df['launch_angle'] - df['release_speed']
    # 5. calculate the game month as a number from 1-12 using the game_date column
    df['game_month'] = pd.DatetimeIndex(df['game_date']).month
    # 6. calculate the number of days since the player's last home run, using the game_date column and the player_id column. For the players first home run, set the value to 0.
    df['days_since_last_hr'] = df.groupby('player_id')['game_date'].diff().dt.days.fillna(0)
    # 7. calculate the cumulative number of home runs across all games for the player up to that point, using the player_id column and a count of rows for the player_id set to 0 for the first home run.
    df['cumulative_hr'] = df.groupby('player_id')['player_id'].cumcount().fillna(0)
    # 8. calculate the moving average for the launch_speed for the player up to that point for the last 20 home runs using the player_id column and for the first 20 home runs, use the average for all home runs for that player.
    # df['moving_avg_speed'] = df.groupby('player_id')['launch_speed'].rolling(20).mean().fillna(df.groupby('player_id')['launch_speed'].mean())
    # 9. calculate the moving average for the launch_angle for the player up to that point for the last 20 home runs using the player_id column and for the first 20 home runs, use the average for all home runs for that player.
    # df['moving_avg_angle'] = df.groupby('player_id')['launch_angle'].rolling(20).mean().fillna(df.groupby('player_id')['launch_angle'].mean())
    # 10. bin the release_speed into 3 bins: slow, normal, fast. These should use the 33rd, 50th, and 67th percentiles
    df['release_speed_bin'] = pd.qcut(df['release_speed'], q=3, labels=['slow', 'normal', 'fast'])
    # 11. create a new feature that combines the "pitch_type" and "p_throws" columns. For example, if the pitch_type is "FF" and the p_throws is "R", the new feature should be "FF_R". If the pitch_type is "FF" and the p_throws is "L", the new feature should be "FF_L" 
    df['pitch_type_p_throws'] = df['pitch_type'] + '_' + df['p_throws']
    # return the dataframe
    return df


# write a test function to test the functions you wrote above
def test():
    # read in the data using the get_all_data function
    df = get_all_data()
    # preprocess the data
    df = preprocess_data(df)
    # add features to the data
    df = add_features(df)
    # check that the preprocess function worked correctly
    assert df['player_name'].isnull().sum() == 0
    print("test 1 complete")
    assert df['player_id'].isnull().sum() == 0
    print("test 2 complete")
    assert df['game_date'].isnull().sum() == 0
    print("test 3 complete")
    assert df['pitch_type'].isnull().sum() == 0
    print("test 4 complete")
    assert df['p_throws'].isnull().sum() == 0
    print("test 5 complete")
    # check that the add_features function worked correctly
    assert df['launch_speed_bin'].isnull().sum() == 0
    print("test 6 complete")
    assert df['launch_angle_bin'].isnull().sum() == 0
    print("test 7 complete")
    assert df['speed_diff'].isnull().sum() == 0
    print("test 8 complete")
    assert df['angle_diff'].isnull().sum() == 0
    print("test 9 complete")
    assert df['game_month'].isnull().sum() == 0
    print("test 10 complete")
    assert df['days_since_last_hr'].isnull().sum() == 0
    print("test 11 complete")
    assert df['cumulative_hr'].isnull().sum() == 0
    print("test 12 complete")
    # assert df['moving_avg_speed'].isnull().sum() == 0
    print("test 13 skipped")
    # assert df['moving_avg_angle'].isnull().sum() == 0
    print("test 14 skipped")
    assert df['release_speed_bin'].isnull().sum() == 0
    print("test 15 complete")
    assert df['pitch_type_p_throws'].isnull().sum() == 0
    print("test 16 complete")
    print('All tests passed!')

# write a function that shows the final dataframe after all the preprocessing and feature engineering steps
def create_final_dataframe():
    # read in the data using the get_all_data function
    df = get_all_data()
    # preprocess the data
    df = preprocess_data(df)
    # add features to the data
    df = add_features(df)
    return df

'''
Write a function that will do all of the following: 
1. Check to see what data is already in the table in BigQuery.
2. If there is no data in the table, load the data into BigQuery.
3. If there is data in the table, only load the data that is not already in the table.
With the conditions below: 
1. The table should be named "homerun_statcast_batter_data_with_features" 
2. It should be in the 'dataset_name' given above.
3. Document the schema in BigQuery with field descriptions. 
4. Include a column for the date the data was added to the table. 
The function should take the dataframe as an argument and return a message with the job id and the number of rows loaded into BigQuery.
'''

def load_data(df):
    # create a BigQuery client
    client = bigquery.Client()
    # create a BigQuery dataset reference
    dataset_ref = client.dataset(dataset_name)
    # create a BigQuery table reference
    table_ref = dataset_ref.table(statcast_batter_table_name + '_with_features')
    # initialize the job variable
    job_result = None
    # define the schema
    schema = [
    bigquery.SchemaField('player_id', 'INTEGER', mode='REQUIRED', description='The id of the player.'),
    bigquery.SchemaField('game_date', 'DATE', mode='REQUIRED', description='The date of the game.'),
    bigquery.SchemaField('player_name', 'STRING', mode='REQUIRED', description='The name of the player.'),
    bigquery.SchemaField('launch_speed', 'FLOAT', mode='REQUIRED', description='The speed of the ball when it leaves the bat.'),
    bigquery.SchemaField('launch_angle', 'FLOAT', mode='REQUIRED', description='The angle of the ball when it leaves the bat.'),
    bigquery.SchemaField('pitch_type', 'STRING', mode='REQUIRED', description='The type of pitch thrown.'),
    bigquery.SchemaField('release_speed', 'FLOAT', mode='REQUIRED', description='The speed of the pitch when it leaves the pitchers hand.'),
    bigquery.SchemaField('p_throws', 'STRING', mode='REQUIRED', description='The hand the pitcher throws with.'),
    bigquery.SchemaField('launch_speed_bin', 'STRING', mode='REQUIRED', description='The speed of the ball when it leaves the bat binned into 3 categories: slow, normal, fast.'),
    bigquery.SchemaField('launch_angle_bin', 'STRING', mode='REQUIRED', description='The angle of the ball when it leaves the bat binned into 3 categories: low, normal, high.'),
    bigquery.SchemaField('speed_diff', 'FLOAT', mode='REQUIRED', description='The difference between the release speed and the launch speed.'),
    bigquery.SchemaField('angle_diff', 'FLOAT', mode='REQUIRED', description='The difference between the release angle and the launch angle.'),
    bigquery.SchemaField('game_month', 'INTEGER', mode='REQUIRED', description='The month of the game.'),
    bigquery.SchemaField('days_since_last_hr', 'INTEGER', mode='REQUIRED', description='The number of days since the player last hit a home run.'),
    bigquery.SchemaField('cumulative_hr', 'INTEGER', mode='REQUIRED', description='The cumulative number of home runs the player has hit in their career.'),
    # bigquery.SchemaField('moving_avg_speed', 'FLOAT', mode='REQUIRED', description='The moving average of the launch speed for the player over the last 10 games.'),
    # bigquery.SchemaField('moving_avg_angle', 'FLOAT', mode='REQUIRED', description='The moving average of the launch angle for the player over the last 10 games.'),
    bigquery.SchemaField('release_speed_bin', 'STRING', mode='REQUIRED', description='The speed of the pitch when it leaves the pitchers hand binned into 3 categories: slow, normal, fast.'),
    bigquery.SchemaField('pitch_type_p_throws', 'STRING', mode='REQUIRED', description='The type of pitch thrown and the hand the pitcher throws with.'),
    bigquery.SchemaField('date_added', 'DATE', mode='REQUIRED', description='The date the data was added to the table.')
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
        print('Loaded {} rows into {}:{}.'.format(job_result.output_rows, dataset_name, statcast_batter_table_name + '_with_features'))
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
        print('Loaded {} rows into {}:{}.'.format(job_result.output_rows, dataset_name, statcast_batter_table_name + '_with_features'))
        return job_result.job_id, job_result.output_rows

# run the test function
test()
# show the final dataframe
create_final_dataframe()
# prepare the data for loading into BigQuery
df = create_final_dataframe()

# load the data into BigQuery
job_id, rows_loaded = load_data(df)

# check the results
print('Job ID: {}'.format(job_id))
print('Rows loaded: {}'.format(rows_loaded))

'''
Using the df we created above, we want to create a new dataframe df2 that summarizes each player's data and the features we just created. 
There should only be one row for each player in the dataframe.

We want to create a new dataframe that has the following information for each player, considering all of the available data for that player:
- player_id
- player_name
- number_of_home_runs
- average_launch_speed
- standard_deviation_launch_speed
- average_launch_angle
- standard_deviation_launch_angle
- average_speed_diff
- average_angle_diff
- average days since last hr
- standard deviation of days since last hr
- average release speed
- median release speed
- proportion of release speed thrown slow, normal, fast (e.g., proportion_of_release_speed_slow)
- proportion of pitch thrown R or L (e.g., proportion_of_pitch_type_R)
- proportion of each pitch type thrown (e.g., proportion_of_pitch_type_FF)
- proportion of each pitch type thrown for each hand (e.g., proportion_of_pitch_type_FF_R)
- proportion_of_game_month (e..g, proportion_of_game_month_4) for months 4,5,6,7,8,9,10

Return the new dataframe
'''

# FIX ME: Does not work, all data is NaN or 0.0
def create_summary_dataframe(df):
    # create a new dataframe with the summary information for each player
    df2 = pd.DataFrame()
    df2['player_id'] = df['player_id'].unique()
    df2['player_name'] = df['player_name']
    df2['number_of_home_runs'] = df.groupby('player_id')['cumulative_hr'].max()
    df2['average_launch_speed'] = df.groupby('player_id')['launch_speed'].mean()
    df2['standard_deviation_launch_speed'] = df.groupby('player_id')['launch_speed'].std()
    df2['average_launch_angle'] = df.groupby('player_id')['launch_angle'].mean()
    df2['standard_deviation_launch_angle'] = df.groupby('player_id')['launch_angle'].std()
    df2['average_speed_diff'] = df.groupby('player_id')['speed_diff'].mean()
    df2['average_angle_diff'] = df.groupby('player_id')['angle_diff'].mean()
    df2['average_days_since_last_hr'] = df.groupby('player_id')['days_since_last_hr'].mean()
    df2['standard_deviation_days_since_last_hr'] = df.groupby('player_id')['days_since_last_hr'].std()
    df2['average_release_speed'] = df.groupby('player_id')['release_speed'].mean()
    df2['median_release_speed'] = df.groupby('player_id')['release_speed'].median()
    df2['proportion_of_release_speed_slow'] = df.groupby('player_id')['release_speed_bin'].value_counts(normalize=True).unstack().loc[:, 'slow']
    df2['proportion_of_release_speed_normal'] = df.groupby('player_id')['release_speed_bin'].value_counts(normalize=True).unstack().loc[:, 'normal']
    df2['proportion_of_release_speed_fast'] = df.groupby('player_id')['release_speed_bin'].value_counts(normalize=True).unstack().loc[:, 'fast']
    df2['proportion_of_pitch_type_R'] = df.groupby('player_id')['p_throws'].value_counts(normalize=True).unstack().loc[:, 'R']
    df2['proportion_of_pitch_type_L'] = df.groupby('player_id')['p_throws'].value_counts(normalize=True).unstack().loc[:, 'L']
    df2['proportion_of_pitch_type_FC'] = df.groupby('player_id')['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'FC']
    df2['proportion_of_pitch_type_SL'] = df.groupby('player_id')['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'SL']
    df2['proportion_of_pitch_type_FF'] = df.groupby('player_id')['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'FF']
    df2['proportion_of_pitch_type_CH'] = df.groupby('player_id')['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'CH']
    df2['proportion_of_pitch_type_SI'] = df.groupby('player_id')['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'SI']
    df2['proportion_of_pitch_type_CU'] = df.groupby('player_id')['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'CU']
    df2['proportion_of_pitch_type_FS'] = df.groupby('player_id')['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'FS']
    df2['proportion_of_pitch_type_KC'] = df.groupby('player_id')['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'KC']
    df2['proportion_of_pitch_type_ST'] = df.groupby('player_id')['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'ST']
    df2['proportion_of_pitch_type_SV'] = df.groupby('player_id')['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'SV']
    df2['proportion_of_pitch_type_EP'] = df.groupby('player_id')['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'EP']
    df2['proportion_of_pitch_type_FA'] = df.groupby('player_id')['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'FA']
    df2['proportion_of_pitch_type_KN'] = df.groupby('player_id')['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'KN']
    df2['proportion_of_pitch_type_CS'] = df.groupby('player_id')['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'CS']
    df2['proportion_of_pitch_type_FC_R'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'FC'].loc[:, 'R']
    df2['proportion_of_pitch_type_SL_R'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'SL'].loc[:, 'R']
    df2['proportion_of_pitch_type_FF_R'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'FF'].loc[:, 'R']
    df2['proportion_of_pitch_type_CH_R'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'CH'].loc[:, 'R']
    df2['proportion_of_pitch_type_SI_R'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'SI'].loc[:, 'R']
    df2['proportion_of_pitch_type_CU_R'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'CU'].loc[:, 'R']
    df2['proportion_of_pitch_type_FS_R'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'FS'].loc[:, 'R']
    df2['proportion_of_pitch_type_KC_R'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'KC'].loc[:, 'R']
    df2['proportion_of_pitch_type_ST_R'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'ST'].loc[:, 'R']
    df2['proportion_of_pitch_type_SV_R'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'SV'].loc[:, 'R']
    df2['proportion_of_pitch_type_EP_R'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'EP'].loc[:, 'R']
    df2['proportion_of_pitch_type_FA_R'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'FA'].loc[:, 'R']
    df2['proportion_of_pitch_type_KN_R'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'KN'].loc[:, 'R']
    df2['proportion_of_pitch_type_CS_R'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'CS'].loc[:, 'R']
    df2['proportion_of_pitch_type_FC_L'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'FC'].loc[:, 'L']
    df2['proportion_of_pitch_type_SL_L'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'SL'].loc[:, 'L']
    df2['proportion_of_pitch_type_FF_L'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'FF'].loc[:, 'L']
    df2['proportion_of_pitch_type_CH_L'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'CH'].loc[:, 'L']
    df2['proportion_of_pitch_type_SI_L'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'SI'].loc[:, 'L']
    df2['proportion_of_pitch_type_CU_L'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'CU'].loc[:, 'L']
    df2['proportion_of_pitch_type_FS_L'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'FS'].loc[:, 'L']
    df2['proportion_of_pitch_type_KC_L'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'KC'].loc[:, 'L']
    df2['proportion_of_pitch_type_ST_L'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'ST'].loc[:, 'L']
    df2['proportion_of_pitch_type_SV_L'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'SV'].loc[:, 'L']
    df2['proportion_of_pitch_type_EP_L'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'EP'].loc[:, 'L']
    df2['proportion_of_pitch_type_FA_L'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'FA'].loc[:, 'L']
    df2['proportion_of_pitch_type_KN_L'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'KN'].loc[:, 'L']
    df2['proportion_of_pitch_type_CS_L'] = df.groupby(['player_id', 'p_throws'])['pitch_type'].value_counts(normalize=True).unstack().loc[:, 'CS'].loc[:, 'L']
    df2['proportion_of_pitch_type_CH_R'] = df2['proportion_of_pitch_type_CH_R'].fillna(0)
    df2['proportion_of_pitch_type_FF_R'] = df2['proportion_of_pitch_type_FF_R'].fillna(0)
    df2['proportion_of_pitch_type_SL_R'] = df2['proportion_of_pitch_type_SL_R'].fillna(0)
    df2['proportion_of_pitch_type_FC_R'] = df2['proportion_of_pitch_type_FC_R'].fillna(0)
    df2['proportion_of_pitch_type_SI_R'] = df2['proportion_of_pitch_type_SI_R'].fillna(0)
    df2['proportion_of_pitch_type_CU_R'] = df2['proportion_of_pitch_type_CU_R'].fillna(0)
    df2['proportion_of_pitch_type_FS_R'] = df2['proportion_of_pitch_type_FS_R'].fillna(0)
    df2['proportion_of_pitch_type_KC_R'] = df2['proportion_of_pitch_type_KC_R'].fillna(0)
    df2['proportion_of_pitch_type_ST_R'] = df2['proportion_of_pitch_type_ST_R'].fillna(0)
    df2['proportion_of_pitch_type_SV_R'] = df2['proportion_of_pitch_type_SV_R'].fillna(0)
    df2['proportion_of_pitch_type_EP_R'] = df2['proportion_of_pitch_type_EP_R'].fillna(0)
    df2['proportion_of_pitch_type_FA_R'] = df2['proportion_of_pitch_type_FA_R'].fillna(0)
    df2['proportion_of_pitch_type_KN_R'] = df2['proportion_of_pitch_type_KN_R'].fillna(0)
    df2['proportion_of_pitch_type_CS_R'] = df2['proportion_of_pitch_type_CS_R'].fillna(0)
    df2['proportion_of_pitch_type_FC_L'] = df2['proportion_of_pitch_type_FC_L'].fillna(0)
    df2['proportion_of_pitch_type_SL_L'] = df2['proportion_of_pitch_type_SL_L'].fillna(0)
    df2['proportion_of_pitch_type_FF_L'] = df2['proportion_of_pitch_type_FF_L'].fillna(0)
    df2['proportion_of_pitch_type_CH_L'] = df2['proportion_of_pitch_type_CH_L'].fillna(0)
    df2['proportion_of_pitch_type_SI_L'] = df2['proportion_of_pitch_type_SI_L'].fillna(0)
    df2['proportion_of_pitch_type_CU_L'] = df2['proportion_of_pitch_type_CU_L'].fillna(0)
    df2['proportion_of_pitch_type_FS_L'] = df2['proportion_of_pitch_type_FS_L'].fillna(0)
    df2['proportion_of_pitch_type_KC_L'] = df2['proportion_of_pitch_type_KC_L'].fillna(0)
    df2['proportion_of_pitch_type_ST_L'] = df2['proportion_of_pitch_type_ST_L'].fillna(0)
    df2['proportion_of_pitch_type_SV_L'] = df2['proportion_of_pitch_type_SV_L'].fillna(0)
    df2['proportion_of_pitch_type_EP_L'] = df2['proportion_of_pitch_type_EP_L'].fillna(0)
    df2['proportion_of_pitch_type_FA_L'] = df2['proportion_of_pitch_type_FA_L'].fillna(0)
    df2['proportion_of_pitch_type_KN_L'] = df2['proportion_of_pitch_type_KN_L'].fillna(0)
    df2['proportion_of_pitch_type_CS_L'] = df2['proportion_of_pitch_type_CS_L'].fillna(0)
    df2['proportion_of_pitch_type_CH'] = df2['proportion_of_pitch_type_CH'].fillna(0)
    df2['proportion_of_pitch_type_FF'] = df2['proportion_of_pitch_type_FF'].fillna(0)
    df2['proportion_of_pitch_type_SL'] = df2['proportion_of_pitch_type_SL'].fillna(0)
    df2['proportion_of_pitch_type_FC'] = df2['proportion_of_pitch_type_FC'].fillna(0)
    df2['proportion_of_pitch_type_SI'] = df2['proportion_of_pitch_type_SI'].fillna(0)
    df2['proportion_of_pitch_type_CU'] = df2['proportion_of_pitch_type_CU'].fillna(0)
    df2['proportion_of_pitch_type_FS'] = df2['proportion_of_pitch_type_FS'].fillna(0)
    df2['proportion_of_pitch_type_KC'] = df2['proportion_of_pitch_type_KC'].fillna(0)
    df2['proportion_of_pitch_type_ST'] = df2['proportion_of_pitch_type_ST'].fillna(0)
    df2['proportion_of_pitch_type_SV'] = df2['proportion_of_pitch_type_SV'].fillna(0)
    df2['proportion_of_pitch_type_EP'] = df2['proportion_of_pitch_type_EP'].fillna(0)
    df2['proportion_of_pitch_type_FA'] = df2['proportion_of_pitch_type_FA'].fillna(0)
    df2['proportion_of_pitch_type_KN'] = df2['proportion_of_pitch_type_KN'].fillna(0)
    df2['proportion_of_pitch_type_CS'] = df2['proportion_of_pitch_type_CS'].fillna(0)

    print(df2.head(10))
    print(df2.shape)
    print(df2.columns)

    return df2

# create_summary_dataframe(df)