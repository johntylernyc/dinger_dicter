import os
from datetime import datetime

from helper_functions.get_qualified_batters import get_qualified_batters
from helper_functions.get_qualified_pitchers import get_qualified_pitchers
from helper_functions.get_batter_data import get_daily_batter_statcast_data 
from helper_functions.gbq_load_batter_statcast_data import load_data_to_gbq as load_batter_data_to_gbq
from helper_functions.profile_batter_data import create_new_batter_df_with_features_and_tests, load_batter_profile_data, create_batter_summary_dataframe, load_batter_summary_data
from helper_functions.profile_pitcher_data import load_pitcher_summary_data, create_new_pitcher_df_with_features_and_tests, load_pitcher_profile_data, create_pitcher_summary_dataframe
from helper_functions.get_probable_starters import get_probable_starters, starters_exist_for_date
from helper_functions.gbq_load_probable_starters import load_probable_starters
from helper_functions.gbq_load_pitcher_statcast_data import load_data_to_gbq as load_pitcher_data_to_gbq
from helper_functions.get_pitcher_data import get_daily_pitcher_statcast_data

from helper_functions.config_bigquery import project_id, dataset_name, statcast_batter_table_name, json_key_path, statcast_pitcher_table_name, probable_pitcher_table_name
from helper_functions.config_application_dates import most_recent_batter_date, yesterday, today_datetime, two_days_ago, batter_date_to_fetch, pitcher_date_to_fetch, today, most_recent_pitcher_date
from helper_functions.config_application_parameters import years, hitter_qual, pitcher_qual

# set date variables being imported from helper_functions.config_application_dates to last 3 days for purpose of testing application
# yesterday = two_days_ago
# today = datetime.strptime(yesterday, '%Y-%m-%d')
# today_datetime = datetime(today.year, today.month, today.day, 9, 0, 0)
# batter_date_to_fetch = yesterday
# pitcher_date_to_fetch = yesterday

def fetch_and_load_batter_statcast_data(date_to_check, date_to_load):
    if datetime.strptime(most_recent_batter_date, '%Y-%m-%d').date() == datetime.strptime(date_to_check, '%Y-%m-%d').date():
        print(f'The most recent date in the batter database is: {most_recent_batter_date}')
        print(f'The most recent available in the batter data is: {date_to_check}')
        print('The most recent date in the batter database is the same as the most recent available data. No need to update batter data.')
        return
    elif datetime.strptime(most_recent_batter_date, '%Y-%m-%d').date() < datetime.strptime(date_to_check, '%Y-%m-%d').date():
        print(f'The most recent date in the batter database is: {most_recent_batter_date}')
        print(f'The most recent available data in the batter data is: {date_to_load}')
        print('New batter data is available. Updating batter data.')
        qualified_batters = get_qualified_batters(years, hitter_qual)
        daily_statcast_data = get_daily_batter_statcast_data(qualified_batters, batter_date_to_fetch, date_to_load)
        # daily_statcast_data_file = 'daily_statcast_data.csv'
        # daily_statcast_data.to_csv(daily_statcast_data_file, index=False, header=False)
        load_batter_data_to_gbq(daily_statcast_data, dataset_name, statcast_batter_table_name, json_key_path, project_id)
        print('The daily batter statcast data has been loaded to BigQuery.')
        df = create_new_batter_df_with_features_and_tests()
        job_id = load_batter_profile_data(df)
        print(f'Job ID: {job_id}')
        print('The batter profile data has been loaded to BigQuery.')
        summary_df = create_batter_summary_dataframe(df)
        job_id, output_rows = load_batter_summary_data(summary_df)
        print(f'Job ID: {job_id}')
        print(f'Output Rows: {output_rows}')
        print('The batter profile summary data has been loaded to BigQuery.')
    else:
        print('Something went wrong updating statcast data. Please check the code.')

if today_datetime.hour < 9:
    fetch_and_load_batter_statcast_data(two_days_ago, yesterday)
elif today_datetime.hour >= 9:
    fetch_and_load_batter_statcast_data(yesterday, yesterday)
else:
    print('Something went terribly wrong updating statcast data. Please check the code.')

print("Kicking off a Node.js application to find today's probable starters.")
probable_starters = get_probable_starters(today)
print("Node.js application has finished. Checking to see if today's probable starters already exist in the database.")
duplicate_count = starters_exist_for_date(today, probable_starters, probable_pitcher_table_name, dataset_name, json_key_path, project_id)
if duplicate_count == 0:
    job_id, output_rows = load_probable_starters(probable_starters, probable_pitcher_table_name, json_key_path, project_id)
    print(f'Job ID: {job_id}')
    print(f'Output Rows: {output_rows}')
    print(f'The probable starters data for {today} has been loaded to BigQuery.')
else:
    print(f'Probable starters for {today} already exist in the database. No need to reload the data.')

def fetch_and_load_pitcher_statcast_data(date_to_check, date_to_load):
    if datetime.strptime(most_recent_pitcher_date, '%Y-%m-%d').date() == datetime.strptime(date_to_check, '%Y-%m-%d').date():
        print(f'The most recent date in the pitcher database is: {most_recent_pitcher_date}')
        print(f'The most recent available in the pitcher data is: {date_to_check}')
        print('The most recent date in the pitcher database is the same as the most recent available data. No need to update pitcher data.')
        return
    elif datetime.strptime(most_recent_pitcher_date, '%Y-%m-%d').date() < datetime.strptime(date_to_check, '%Y-%m-%d').date():
        print(f'The most recent date in the pitcher database is: {most_recent_pitcher_date}')
        print(f'The most recent available data in the pitcher data is: {date_to_load}')
        print('New pitcher data is available. Updating pitcher data.')
        qualified_pitchers = get_qualified_pitchers(years, hitter_qual)
        daily_pitcher_statcast_data = get_daily_pitcher_statcast_data(qualified_pitchers, pitcher_date_to_fetch, date_to_load)
        # daily_statcast_data_file = 'daily_statcast_data.csv'
        # daily_statcast_data.to_csv(daily_statcast_data_file, index=False, header=False)
        load_pitcher_data_to_gbq(daily_pitcher_statcast_data, dataset_name, statcast_pitcher_table_name, json_key_path, project_id)
        print('The daily pitcher statcast data has been loaded to BigQuery.')
        df = create_new_pitcher_df_with_features_and_tests()
        job_id = load_pitcher_profile_data(df)
        print(f'Job ID: {job_id}')
        print('The pitcher profile data has been loaded to BigQuery.')
        summary_df = create_pitcher_summary_dataframe(df)
        job_id, output_rows = load_pitcher_summary_data(summary_df)
        print(f'Job ID: {job_id}')
        print(f'Output Rows: {output_rows}')
        print('The pitcher profile summary data has been loaded to BigQuery.')
    else:
        print('Something went wrong updating statcast data. Please check the code.')

if today_datetime.hour < 9:
    fetch_and_load_pitcher_statcast_data(two_days_ago, yesterday)
elif today_datetime.hour >= 9:
    fetch_and_load_pitcher_statcast_data(yesterday, yesterday)
else:
    print('Something went terribly wrong updating statcast data. Please check the code.')

# qualified_pitchers = get_qualified_pitchers(years, pitcher_qual)
# daily_statcast_data = get_daily_pitcher_statcast_data(qualified_pitchers, pitcher_date_to_fetch, today)
# load_pitcher_data_to_gbq(daily_statcast_data, dataset_name, statcast_pitcher_table_name, json_key_path, project_id)