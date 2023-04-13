from pybaseball import statcast_pitcher
import pandas as pd

date_to_fetch = ''
today = ''

def get_daily_statcast_data(player_ids, start_date=date_to_fetch, end_date=today):
    daily_statcast_data = []
    for player in player_ids:
        daily_statcast_data.append(statcast_pitcher(start_date, end_date, player))
        daily_statcast_data[-1]['player_id'] = player
    daily_statcast_data = pd.concat(daily_statcast_data)
    daily_statcast_data = daily_statcast_data[daily_statcast_data['game_type'] == 'R']
    return daily_statcast_data

# if __name__ == '__main__':
#     player_ids = [657248, 608566, 621219]
#     date_to_fetch = '2023-04-01'
#     today = '2023-04-11'
#     result = get_daily_statcast_data(player_ids, date_to_fetch, today)
#     # result.to_csv('test_data_fetch.csv', index=False, header=True)
#     print(result)
#     print(result.columns)
#     print(result.info())