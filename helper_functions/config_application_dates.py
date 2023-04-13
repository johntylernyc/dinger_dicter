from datetime import date, timedelta, datetime
from .gbq_lookup_most_recent_data import get_most_recent_date
from .config_bigquery import project_id, dataset_name, statcast_batter_table_name, json_key_path

# set today as today's date in the %Y-%m-%d format
today = date.today().strftime("%Y-%m-%d")
# get today's date and currrent time
today_datetime = datetime.now()
# set yesterday using today's date in the %Y-%m-%d format
yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
# two days ago
two_days_ago = (date.today() - timedelta(days=2)).strftime("%Y-%m-%d")
# get the most recent date that we've loaded statcast data
most_recent_date = get_most_recent_date(project_id, dataset_name, statcast_batter_table_name, json_key_path)
# using most_recent_date, set date_to_fetch as tomorrow in the %Y-%m-%d format
date_to_fetch = (date.fromisoformat(most_recent_date) + timedelta(days=1)).strftime("%Y-%m-%d")