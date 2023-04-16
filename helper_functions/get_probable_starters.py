import os
import subprocess
import json
from pybaseball import playerid_lookup, playerid_reverse_lookup
import pandas as pd

def run_npm_start():
    try:
        project_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'baseball-probable-pitchers')
        process = subprocess.Popen(["npm", "start"], cwd=project_path, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        print(f"Success: {stdout.decode('utf-8')}") if process.returncode == 0 else print(f"Error: {stderr.decode('utf-8')}")
    except Exception as e:
        print(f"An error occurred: {e}")

def get_probable_starters():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_path = os.path.join(script_dir, '..', 'baseball-probable-pitchers')
    probable_pitchers_file = os.path.join(project_path, 'probable-pitchers.json')

    with open(probable_pitchers_file, 'r') as f:
        json_data = json.load(f)
        probable_pitchers_data = json_data['matchups']

    probable_pitchers = pd.DataFrame(probable_pitchers_data)
    away_player_mlbam = []
    home_player_mlbam = []

    def get_player_mlbam(player_name):
        first_name, last_name = player_name.split(' ')
        player_mlbam = playerid_lookup(last_name, first_name, fuzzy=True)
        return str(player_mlbam['key_mlbam'].values[0]) if not player_mlbam.empty else None

    for index, row in probable_pitchers.iterrows():
        away_player_mlbam.append(get_player_mlbam(row['pitcherAway']))
        home_player_mlbam.append(get_player_mlbam(row['pitcherHome']))

    probable_pitchers['away_player_key_mlbam'] = away_player_mlbam
    probable_pitchers['home_player_key_mlbam'] = home_player_mlbam

    probable_pitchers_with_player_ids_file = os.path.join(project_path, 'probable-pitchers-with-player-ids.json')
    probable_pitchers.to_json(probable_pitchers_with_player_ids_file, orient='records')

    return probable_pitchers

if __name__ == "__main__":
    run_npm_start()
    probable_pitchers = get_probable_starters()
    print(probable_pitchers) 