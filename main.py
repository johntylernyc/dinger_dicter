import os
from datetime import datetime, timedelta, date

from helper_functions.get_qualified_batters import get_qualified_batters
from helper_functions.get_qualified_pitchers import get_qualified_pitchers
from helper_functions.get_batter_data import get_daily_batter_statcast_data
from helper_functions.gbq_load_batter_statcast_data import load_data_to_gbq as load_batter_data_to_gbq
from helper_functions.profile_batter_data import create_new_batter_df_with_features_and_tests, load_batter_profile_data, create_batter_summary_dataframe, load_batter_summary_data
from helper_functions.profile_pitcher_data import load_pitcher_summary_data, create_new_pitcher_df_with_features_and_tests, load_pitcher_profile_data, create_pitcher_summary_dataframe
from helper_functions.get_probable_starters import get_probable_starters
from helper_functions.gbq_load_probable_starters import load_probable_starters
from helper_functions.gbq_load_pitcher_statcast_data import load_data_to_gbq as load_pitcher_data_to_gbq
from helper_functions.get_pitcher_data import get_daily_pitcher_statcast_data
from helper_functions.config_bigquery import project_id, dataset_name, statcast_batter_table_name, json_key_path, statcast_pitcher_table_name, probable_pitcher_table_name
from helper_functions.config_application_dates import most_recent_batter_date, yesterday, today_datetime, two_days_ago, batter_date_to_fetch, pitcher_date_to_fetch, today, most_recent_pitcher_date
from helper_functions.config_application_parameters import years, hitter_qual, pitcher_qual

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_key_path

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

#TODO: Replace these variables with real dates from the probable_pitchers_daily table
start_date_probable_pitchers = date(2023, 3, 30)  # the day you want to retrieve records
end_date_probable_pitchers = date(2023, 5, 30)  # the day after the last day you want to retrieve

print("Kicking off a Node.js application to find today's probable starters.")
def daterange(start_date_probable_pitchers, end_date_probable_pitchers):
    for n in range(int((end_date_probable_pitchers - start_date_probable_pitchers).days)):
        yield start_date_probable_pitchers + timedelta(n)


for single_date in daterange(start_date_probable_pitchers, end_date_probable_pitchers):
    date_to_fetch = single_date.strftime("%Y-%m-%d")
    print(f"Fetching probable starters for date: {date_to_fetch}")
    # run_npm_start(date_to_fetch)
    probable_starters = get_probable_starters(date_to_fetch)
    if probable_starters is None:
        print("No data available. Skipping load job.")
        continue
    else:
        print(f"Probable starters for date: {date_to_fetch} have been fetched.")
        # assuming you have a function to save/load these to a database
        load_probable_starters(probable_starters, probable_pitcher_table_name, json_key_path)
        print(f"Probable starters for date: {date_to_fetch} have been loaded to BigQuery.")

# probable_starters = get_probable_starters(today)
# print("Node.js application has finished. Checking to see if today's probable starters already exist in the database.")
# load_probable_starters(probable_starters, probable_pitcher_table_name, json_key_path)
# print("Today's probable starters have been loaded to BigQuery.")
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
        qualified_pitchers = get_qualified_pitchers(years, pitcher_qual)
        daily_pitcher_statcast_data = get_daily_pitcher_statcast_data(qualified_pitchers, pitcher_date_to_fetch, date_to_load)
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
    print('Something went wrong updating statcast data. Please check the code.')