player_ids = [592450, 623993]

from helper_functions.get_batter_data import get_daily_statcast_data
from helper_functions.config_application_dates import most_recent_batter_date, today, yesterday

batter_data = get_daily_statcast_data(player_ids, start_date=most_recent_batter_date, end_date=yesterday)
print(batter_data)
print("yesterday:")
print(yesterday)
print("today:")
print(today)
print("date_to_fetch:")
print(most_recent_batter_date)