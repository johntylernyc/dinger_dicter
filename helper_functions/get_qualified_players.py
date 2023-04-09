# Purpose: Get the list of qualified players for each year in the `years` list using the `batting_stats` function from pybaseball.
# File Name: get_qualified_players.py
# File Path: helper_functions/get_qualified_players.py
# Author: John Tyler
# Date Created: 2023-04-08

from pybaseball import batting_stats
from pybaseball import playerid_reverse_lookup

def get_qualified_players(years, qual):
    player_ids_list = []
    for year in years: 
        # get the list of qualified players
        qualified_players = batting_stats(year, qual=qual)
        # get the `player_ids` for each player in the `qualified_players` dataframe
        player_ids = playerid_reverse_lookup(qualified_players['IDfg'].values, key_type='fangraphs')
        # transform the `player_ids` list into a list of of the `key_mlbam` values
        player_ids = player_ids['key_mlbam'].values.tolist()
        #add player_ids to player_ids_list
        player_ids_list.extend(player_ids)
    return player_ids_list