# import the required libraries
import os 
from datetime import datetime

# imoprt the functions from the helper_functions folder
from helper_functions.get_qualified_batters import get_qualified_players
from helper_functions.get_batter_data import get_daily_statcast_data
from helper_functions.gbq_load_batter_statcast_data import load_batter_data_to_gbq
from helper_functions.profile_batter_data import create_new_batter_df_with_features_and_tests, load_batter_profile_data, create_batter_summary_dataframe, load_batter_summary_data

# import configuration variables from the configuration files
from helper_functions.config_bigquery import project_id, dataset_name, statcast_batter_table_name, json_key_path
from helper_functions.config_application_dates import most_recent_date, date_to_fetch, yesterday, today_datetime, two_days_ago
from helper_functions.config_application_parameters import years, hitter_qual

# if today_datetime is before 12:00 PM, check to see if most_recent_date converted to a date object is == to two_days_ago converted to a date object
if today_datetime.hour < 12:
    if datetime.strptime(most_recent_date, '%Y-%m-%d').date() == datetime.strptime(two_days_ago, '%Y-%m-%d').date():
        print('The most recent date in the database is: ' + most_recent_date)
        print('The most recent available data is: ' + two_days_ago)
        print('The most recent date in the database is the same as the most recent available data. No need to run the application.')
        exit()
    elif datetime.strptime(most_recent_date, '%Y-%m-%d').date() < datetime.strptime(two_days_ago, '%Y-%m-%d').date():
        print('The most recent date in the database is: ' + most_recent_date)
        print('The most recent available data is: ' + yesterday)
        print('New data is available. Running the application.')
        # get the list of qualified players
        qualified_players = get_qualified_players(years, hitter_qual)
        # get the daily statcast data for each player in the list of qualified players
        daily_statcast_data = get_daily_statcast_data(qualified_players, date_to_fetch, yesterday)
        # write the daily statcast data to a csv file, removing the header row
        daily_statcast_data_file = 'daily_statcast_data.csv'
        daily_statcast_data.to_csv(daily_statcast_data_file, index=False, header=False)
        # load daily_statcast_data.csv to BigQuery using the load_batter_data_to_gbq function from helper_functions/load_batter_data_to_gbq.py
        load_batter_data_to_gbq(daily_statcast_data_file, dataset_name, statcast_batter_table_name, json_key_path, project_id)
        # delete the daily_statcast_data.csv file
        os.remove(daily_statcast_data_file)
        # print a success message
        print('The daily batter statcast data has been loaded to BigQuery.')
    else: 
        print('The hour is before noon but something went wrong updating statcast data. Please check the code.')
        exit()
elif today_datetime.hour >= 12:
    if datetime.strptime(most_recent_date, '%Y-%m-%d').date() == datetime.strptime(yesterday, '%Y-%m-%d').date():
        print('The most recent date in the database is: ' + most_recent_date)
        print('The most recent available data is: ' + yesterday)
        print('The most recent date in the database is the same as the most recent available data. No need to run the application.')
        exit()
    elif datetime.strptime(most_recent_date, '%Y-%m-%d').date() < datetime.strptime(yesterday, '%Y-%m-%d').date():
        print('The most recent date in the database is: ' + most_recent_date)
        print('The most recent available data is: ' + yesterday)
        print('New data is available. Running the application.')
        # get the list of qualified players
        qualified_players = get_qualified_players(years, hitter_qual)
        # get the daily statcast data for each player in the list of qualified players
        daily_statcast_data = get_daily_statcast_data(qualified_players, date_to_fetch, yesterday)
        # write the daily statcast data to a csv file, removing the header row
        daily_statcast_data_file = 'daily_statcast_data.csv'
        daily_statcast_data.to_csv(daily_statcast_data_file, index=False, header=False)
        # load daily_statcast_data.csv to BigQuery using the load_batter_data_to_gbq function from helper_functions/load_batter_data_to_gbq.py
        load_batter_data_to_gbq(daily_statcast_data_file, dataset_name, statcast_batter_table_name, json_key_path, project_id)
        # delete the daily_statcast_data.csv file
        os.remove(daily_statcast_data_file)
        # print a success message
        print('The daily batter statcast data has been loaded to BigQuery.')
    else: 
        print('The hour is past noon but something went wrong updating statcast data. Please check the code.')
        exit()
else:
    print('The hour is neither before nor after noon. Something went terribly wrong updating statcast data. Please check the code.')
    exit()

# profile the hitters and load the results to BigQuery
df = create_new_batter_df_with_features_and_tests()
job_id = load_batter_profile_data(df)
print('Job ID: {}'.format(job_id))
print('The hitter profile data has been loaded to BigQuery.')
# create the summary dataframe
summary_df = create_batter_summary_dataframe(df)
job_id, output_rows = load_batter_summary_data(summary_df)
print('Job ID: {}'.format(job_id))
print('Output Rows: {}'.format(output_rows))
print('The hitter profile summary data has been loaded to BigQuery.')

exit()