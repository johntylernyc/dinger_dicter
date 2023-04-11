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

# import configuration variables from the config.py file
from helper_functions.config import project_id, dataset_name, statcast_batter_table_name, json_key_path, years, qual, date_to_fetch, today, most_recent_date

if most_recent_date == today:
    print('The most recent date in the database is the same as today. No need to run the application.')
    exit()
elif most_recent_date != today:
    print('The most recent date in the database is not the same as today. Running the application.')

    # get the list of qualified players
    qualified_players = get_qualified_players(years, qual)

    # get the daily statcast data for each player in the list of qualified players
    daily_statcast_data = get_daily_statcast_data(qualified_players, date_to_fetch, today)

    # write the daily statcast data to a csv file, removing the header row
    daily_statcast_data_upload = daily_statcast_data.to_csv('daily_statcast_data.csv', index=False, header=False)

    # load daily_statcast_data.csv to BigQuery using the load_batter_data_to_gbq function from helper_functions/load_batter_data_to_gbq.py
    load_batter_data_to_gbq(daily_statcast_data_upload, dataset_name, statcast_batter_table_name, json_key_path)
    exit()
else:
    print('Something went wrong. Please check the code.')
    exit()