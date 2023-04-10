# Purpose: Get the daily statcast data for each player in the `player_ids` list using the `statcast_batter` function from pybaseball.
# File Name: get_batter_data.py
# File Path: helper_functions/get_batter_data.py
# Author: John Tyler
# Created: 2023-04-08

from pybaseball import statcast_batter
import pandas as pd

# create variables that will be passed in as empty strings
most_recent_date = ''
yesterday = ''

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

if __name__ == "__main__":
    arg1_value = [592450, 623993]
    arg2_value = '2021-01-01'
    result = get_daily_statcast_data(arg1_value, arg2_value)
    result.to_csv('test_data_fetch.csv', index=False, header=True)
    print(result)