from pybaseball import statcast_batter
import pandas as pd
from .config_application_dates import date_to_fetch, yesterday

def get_daily_statcast_data(player_ids, start_date=date_to_fetch, end_date=yesterday):
    # create an empty list to store the daily statcast data for each player
    daily_statcast_data = []
    # loop through each player in the `player_ids` list
    for player in player_ids:
        # get the daily statcast data for each player
        daily_statcast_data.append(statcast_batter(start_date, end_date, player))
        # append the player's id to the `daily_statcast_data` dataframe in the 1st column
        daily_statcast_data[-1]['player_id'] = player
    # concatenate the list of dataframes into a single dataframe
    daily_statcast_data = pd.concat(daily_statcast_data)
    # subset the dataframe to only include rows where the `events` column is `home_run`
    daily_statcast_data = daily_statcast_data[daily_statcast_data['events'] == 'home_run']
    # subset the dataframe to only include rows where the `game_type` column is `R`
    daily_statcast_data = daily_statcast_data[daily_statcast_data['game_type'] == 'R']
    # subset the dataframe to only include the required columns
    daily_statcast_data = daily_statcast_data[['player_id', 'game_date', 'player_name','launch_speed', 'launch_angle', 'pitch_type', 'release_speed', 'p_throws']]
    # return the dataframe
    return daily_statcast_data