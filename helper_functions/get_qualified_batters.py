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
    
# if __name__ == "__main__":
#     arg1_value = [2022, 2021]
#     arg2_value = 300
#     result = get_qualified_players(arg1_value, arg2_value)
#     print(result)