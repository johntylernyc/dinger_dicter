# Purpose: Get the list of qualified players for each year in the `years` list using the `batting_stats` function from pybaseball.
# File Name: get_qualified_players.py
# File Path: helper_functions/get_qualified_players.py
# Author: John Tyler
# Created: 2023-04-08

from pybaseball import batting_stats
from pybaseball import playerid_reverse_lookup

#TODO: We might want players who averaged X or more plate appareances over the years given as opposed to adding any player that hit that minimum in one year. 

def get_qualified_players(years, qual):
    player_ids_list = []
    for year in years: 
        # get the list of qualified players
        qualified_players = batting_stats(year, qual=qual)
        # get the `player_ids` for each player in the `qualified_players` dataframe
        player_ids = playerid_reverse_lookup(qualified_players['IDfg'].values, key_type='fangraphs')
        # transform the `player_ids` list into a list of of the `key_mlbam` values
        player_ids = player_ids['key_mlbam'].values.tolist()
        # add player_ids to player_ids_list
        player_ids_list.extend(player_ids)
        # remove duplicates from player_ids_list
        player_ids_list = list(dict.fromkeys(player_ids_list))
    return player_ids_list
    
if __name__ == "__main__":
    arg1_value = [2022, 2021]
    arg2_value = 300
    result = get_qualified_players(arg1_value, arg2_value)
    print(result)

'''
This function was returning duplicates in the player_ids_list. Appears to have been fixed. Testing/troubleshooting notes below.
--------------------------------------------------
Example of most recent data load on 4/8/23: 
home_runs_hit, player_name, year	
136, Judge, Aaron, 2022-01-01 <-- Judge has only hit 62 home runs in 2022
76, Judge, Aaron, 2021-01-01 <-- Judge has only hit 39 home runs in 2021
8, Judge, Aaron, 2023-01-01 <-- as of this date, Judge has only hit 3 home runs in 2023

Started by testing the get_batter_data.py function
--------------------------------------------------
Running the `pybaseball` functions manually I'm getting the right number of home runs per 
season and the regular season filter is working correctly. It's likely an issue with the 
append function in this function or something similar.

Weird, I've also tested pulling just 5 batters and am getting the right inputs into 
the database for all. 

When I reran the entire universe, I'm getting 124, 78, and 4. It looks like it's double counting home runs so 1 
= 2 and 2 = 4. 

Ran the test case for this function and it's working as expected for one player. Is it when I iterate between
players? Yep. Working

Took list of qualified players and that's where the duplicates are occurring. Players that had multiple seasons
with 300 or more plate appearances.
- 192 duplicate rows found and removed.
- 347 unique rows remain.

Testing for the get_qualified_players.py function
--------------------------------------------------
Think about how we want to handle qualified apparances. Currently, the function adds any player 
that had 300 qualifying appearances in any of the years given to the list of player ids. And adds the player id 
each time they had 300 or more plate appearances in a given year. 

- Yes, we can remove the duplicates. But this still doesn't neccessarily give us the desired list.
--- I've temporarily removed duplicates for the purposes of just confirming that is the underlying issue. (4/9/23)
--- It appears this has worked! Should do some additional testing to confirm. (4/10/23)
'''