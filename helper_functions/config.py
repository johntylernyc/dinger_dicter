from datetime import date
from datetime import timedelta
from gbq_lookup_most_recent_data import get_most_recent_date

# define a list of years that will be used to get the list of qualified players
years = [2021,2022]
# define the minimum number of plate appearances that will be used to get the list of qualified players
qual = 300

# set variables for bigquery project and dataset. 
project_id = 'python-sandbox-381204'
dataset_name = 'dinger_dicter'
statcast_batter_table_name = 'homerun_batter_statcast_data'
json_key_path = '/Users/johntyler/Documents/GitHub/dinger_dicter/service_account_keys/python-sandbox-381204-18d99cdada13.json'

# set today as today's date in the %Y-%m-%d format
today = date.today().strftime("%Y-%m-%d")
# set yesterday using today's date in the %Y-%m-%d format
yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
# get the most recent date that we've loaded statcast data
most_recent_date = get_most_recent_date(project_id, dataset_name, statcast_batter_table_name, json_key_path)
# using most_recent_date, set date_to_fetch as tomorrow in the %Y-%m-%d format
date_to_fetch = (date.fromisoformat(most_recent_date) + timedelta(days=1)).strftime("%Y-%m-%d")