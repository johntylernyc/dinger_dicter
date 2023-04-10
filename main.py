# File Name: main.py
# File Path: main.py
# Author: John Tyler
# Created: 2023-04-08

"""
This script will get the list of qualified players for the 2021 and 2022 seasons, get the daily statcast data for each 
player in the list of qualified players, and load the daily statcast data to BigQuery.
"""

from helper_functions.get_qualified_players import get_qualified_players
from helper_functions.get_batter_data import get_daily_statcast_data
from helper_functions.gbq_load_batter_statcast_data import load_batter_data_to_gbq
from helper_functions.gbq_lookup_most_recent_data import get_most_recent_date
from datetime import date
from datetime import timedelta

# define a list of years that will be used to get the list of qualified players
years = [2021,2022]
# define the minimum number of plate appearances that will be used to get the list of qualified players
qual = 300
# set variables for bigquery project and dataset. 
project_id = 'python-sandbox-381204'
dataset_name = 'dinger_dicter'
statcast_table_name = 'homerun_statcast_data'
json_key_path = 'service_account_keys/python-sandbox-381204-18d99cdada13.json'
# set a variety of dates for determining which code to run and which data to pull. 
today = date.today()
yesterday = today - timedelta(days=1)
yesterday = yesterday.strftime("%Y-%m-%d")
most_recent_date = get_most_recent_date(project_id, dataset_name, statcast_table_name, json_key_path)

if most_recent_date == today:
    print('The most recent date in the database is the same as today. No need to run the application.')
    exit()
elif most_recent_date != today:
    print('The most recent date in the database is not the same as today. Running the application.')

    # get the list of qualified players
    qualified_players = get_qualified_players(years, qual)

    # get the daily statcast data for each player in the list of qualified players
    daily_statcast_data = get_daily_statcast_data(qualified_players, most_recent_date, yesterday)

    # write the daily statcast data to a csv file, removing the header row
    daily_statcast_data_upload = daily_statcast_data.to_csv('daily_statcast_data.csv', index=False, header=False)

    # load daily_statcast_data.csv to BigQuery using the load_batter_data_to_gbq function from helper_functions/load_batter_data_to_gbq.py
    load_batter_data_to_gbq(daily_statcast_data_upload, project_id, dataset_name, statcast_table_name, json_key_path)
    exit()
else:
    print('Something went wrong. Please check the code.')
    exit()