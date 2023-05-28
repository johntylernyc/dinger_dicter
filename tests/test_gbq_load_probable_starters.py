import pandas as pd
import json

json_file = '/Users/johntyler/Documents/GitHub/dinger_dicter/helper_functions/baseball-probable-pitchers/probable-pitchers-with-player-ids.json'

try:
    with open(json_file, 'r') as file:
        json_data = file.read()
        parsed_data = json.loads(json_data)
        probable_pitchers = pd.DataFrame(parsed_data)
except ValueError as e:
    print(f"Error reading JSON file {json_file}: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")