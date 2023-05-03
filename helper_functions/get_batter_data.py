from pybaseball import statcast_batter
import pandas as pd
from .config_application_dates import batter_date_to_fetch, yesterday

def get_daily_statcast_data(player_ids, start_date=batter_date_to_fetch, end_date=yesterday):
    daily_statcast_data = []
    for player in player_ids:
        try:
            player_data = statcast_batter(start_date, end_date, player)
            player_data['player_id'] = player
            daily_statcast_data.append(player_data)
        except pd.errors.ParserError:
            print(f"Error parsing data for player {player}. Skipping this player.")
    daily_statcast_data = pd.concat(daily_statcast_data)   
    daily_statcast_data = daily_statcast_data[daily_statcast_data['events'] == 'home_run']
    daily_statcast_data = daily_statcast_data[daily_statcast_data['game_type'] == 'R']
    daily_statcast_data = daily_statcast_data[['player_id', 'game_date', 'player_name','launch_speed', 'launch_angle', 'pitch_type', 'release_speed', 'p_throws', 'release_spin_rate']]
    return daily_statcast_data