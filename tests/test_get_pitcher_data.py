from helper_functions.get_pitcher_data import get_daily_statcast_data
from helper_functions.config_application_dates import pitcher_date_to_fetch, today, yesterday

player_ids = [657248, 608566, 621219]

pitcher_data = get_daily_statcast_data(player_ids, start_date=pitcher_date_to_fetch, end_date=yesterday)
print(pitcher_data)
print("yesterday:")
print(yesterday)
print("today:")
print(today)
print("date_to_fetch:")
print(pitcher_date_to_fetch)