# Purpose: Get the daily statcast data for each player in the `player_ids` list using the `statcast_batter` function from pybaseball.
# File Name: get_batter_data.py
# File Path: helper_functions/get_batter_data.py
# Author: John Tyler
# Date Created: 2023-04-08

from pybaseball import statcast_batter
from datetime import timedelta
from datetime import date
import pandas as pd

# get today's date
today = date.today()
# get yesterday's date
yesterday = today - timedelta(days=1)
# convert `yesterday` to a string
yesterday = yesterday.strftime("%Y-%m-%d")
# create most_recent_date variable as an empty string
most_recent_date = ''

# FIXME: This function is not getting the right number of home runs. Need to fix this.
'''
Example of most recent data load on 4/8/23: 
home_runs_hit, player_name, year	
136, Judge, Aaron, 2022-01-01 <-- Judge has only hit 62 home runs in 2022
76, Judge, Aaron, 2021-01-01 <-- Judge has only hit 39 home runs in 2021
8, Judge, Aaron, 2023-01-01 <-- as of this date, Judge has only hit 3 home runs in 2023

Running the `pybaseball` functions manually I'm getting the right number of home runs per 
season and the regular season filter is working correctly. It's likely an issue with the 
append function in this function or something similar.

Weird, I've also tested pulling just 5 batters and am getting the right inputs into 
the database for all. 
'''

def get_daily_statcast_data(player_ids, start_date=most_recent_date, end_date=yesterday):
    # create an empty list to store the daily statcast data for each player
    daily_statcast_data = []
    # loop through each player in the `player_ids` list
    for player in player_ids:
        # get the daily statcast data for each player
        daily_statcast_data.append(statcast_batter(start_date, end_date, player))
    # concatenate the list of dataframes into a single dataframe
    daily_statcast_data = pd.concat(daily_statcast_data)
    # subset the dataframe to only include rows where the `events` column is `home_run`
    daily_statcast_data = daily_statcast_data[daily_statcast_data['events'] == 'home_run']
    # subset the dataframe to only include rows where the `game_type` column is `R`
    daily_statcast_data = daily_statcast_data[daily_statcast_data['game_type'] == 'R']
    # subset the dataframe to only include the required columns
    daily_statcast_data = daily_statcast_data[['game_date', 'player_name','launch_speed', 'launch_angle', 'pitch_type', 'release_speed', 'p_throws']]
    # return the dataframe
    return daily_statcast_data