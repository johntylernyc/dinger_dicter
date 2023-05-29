from helper_functions.get_probable_starters import get_probable_starters, run_npm_start
from helper_functions.gbq_load_probable_starters import load_probable_starters
from helper_functions.config_bigquery import json_key_path, probable_pitcher_table_name
from datetime import timedelta, date


start_date = date(2022, 4, 9)
end_date = date(2022, 4, 10)
# actual end_date was 2022, 10, 2 but I'm using 2022, 4, 8 for testing purposes after git merge and submodule cleanup

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


for single_date in daterange(start_date, end_date):
    date_to_fetch = single_date.strftime("%Y-%m-%d")
    print(f"Fetching probable starters for date: {date_to_fetch}")
    # run_npm_start(date_to_fetch)
    probable_starters = get_probable_starters(date_to_fetch)
    if probable_starters.empty:
        print("No data available. Skipping load job.")
        continue
    else:
        print(f"Probable starters for date: {date_to_fetch} have been fetched.")
        # assuming you have a function to save/load these to a database
        load_probable_starters(probable_starters, probable_pitcher_table_name, json_key_path)
        print(f"Probable starters for date: {date_to_fetch} have been loaded to BigQuery.")