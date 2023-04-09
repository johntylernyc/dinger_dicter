# daily_batting_stats.py

from pybaseball import statcast_batter
import pandas as pd 

# Retrieve data for each player separately
player_1_data = statcast_batter('2021-01-01', '2023-04-07', player_id=624413)
player_2_data = statcast_batter('2021-01-01', '2023-04-07', player_id=665742)
player_3_data = statcast_batter('2021-01-01', '2023-04-07', player_id=521692)

# Concatenate the dataframes
batter_daily_data_2021_to_2023 = pd.concat([player_1_data, player_2_data, player_3_data], ignore_index=True)


filtered_data = batter_daily_data_2021_to_2023[batter_daily_data_2021_to_2023['game_year'].isin([2021, 2022])]
at_bats_per_game = filtered_data.groupby(['batter', 'game_year', 'game_pk']).agg({'events': 'count'}).reset_index()
average_at_bats = at_bats_per_game.groupby(['batter', 'game_year']).agg({'events': 'mean'}).reset_index()
players_with_min_at_bats_2021 = average_at_bats[(average_at_bats['game_year'] == 2021) & (average_at_bats['events'] >= 3.1)]
players_with_min_at_bats_2022 = average_at_bats[(average_at_bats['game_year'] == 2022) & (average_at_bats['events'] >= 3.1)]

qualified_players = pd.merge(players_with_min_at_bats_2021['batter'], players_with_min_at_bats_2022['batter'], on='batter', how='inner')

qualified_players_data = batter_daily_data_2021_to_2023[batter_daily_data_2021_to_2023['batter'].isin(qualified_players['batter'])]

#At bats player hit a home run
home_runs_data = qualified_players_data[qualified_players_data['events'] == 'home_run']

at_bats_data = qualified_players_data[qualified_players_data['events'].notnull()]
total_at_bats = at_bats_data.groupby('batter').agg({'events': 'count'}).reset_index()
total_home_runs = home_runs_data.groupby('batter').agg({'events': 'count'}).reset_index()
merged_data = pd.merge(total_at_bats, total_home_runs, on='batter', suffixes=('_at_bats', '_home_runs'))
merged_data['average_at_bats_per_home_run'] = merged_data['events_at_bats'] / merged_data['events_home_runs']

# Calculate the number of games played by each player
games_played = qualified_players_data.groupby(['player_name', 'game_date']).size().reset_index(name='count').groupby('player_name').size()

# Calculate the total number of home runs per player
home_runs_per_player = qualified_players_data.groupby('player_name').size()

# Calculate the rolling average of home runs per game (using a window of 10 games)
rolling_average_window = 10
rolling_average = home_runs_per_player / games_played * rolling_average_window

def at_bats_between_hrs(player_data):
    player_data['at_bat_number_shifted'] = player_data['at_bat_number'].shift(1)
    player_data['at_bats_between_hrs'] = player_data['at_bat_number'] - player_data['at_bat_number_shifted'] - 1
    return player_data.iloc[1:]  # Exclude the first row as it doesn't have a valid value for at_bats_between_hrs

# Calculate at bats between home runs for each player
qualified_players_data = qualified_players_data.groupby('player_name').apply(at_bats_between_hrs)

# Calculate the standard deviation of at bats per home run for each player
standard_deviation = qualified_players_data.reset_index(drop=True).groupby('player_name')['at_bats_between_hrs'].std()

qualified_players_data = qualified_players_data.reset_index(drop=True)
qualified_players_data['game_date'] = pd.to_datetime(qualified_players_data['game_date'])

# Filter players who haven't hit a home run in the last 2 days
qualified_players_data = qualified_players_data.groupby('player_name').filter(lambda x: x['game_date'].max() < pd.Timestamp.now() - pd.to_timedelta("2 days"))

# Set a threshold for recent rolling average
rolling_average_threshold = 0.2

# Find players who meet the criteria
candidates = rolling_average[(rolling_average > rolling_average_threshold) & (qualified_players_data.groupby('player_name')['game_date'].max() < pd.Timestamp.now() - pd.to_timedelta("2 days"))]

# Combine the rolling average and standard deviation data
player_metrics = pd.concat([rolling_average, standard_deviation], axis=1)
player_metrics.columns = ['rolling_average', 'standard_deviation']

# Filter based on the identified candidates
player_metrics = player_metrics.loc[candidates.index]

# Sort by rolling average and standard deviation
player_metrics = player_metrics.sort_values(by=['rolling_average', 'standard_deviation'], ascending=[False, True])

# Display the results
print(player_metrics)
