from pybaseball import batting_stats
from pybaseball import playerid_reverse_lookup


def get_qualified_batters(years, qual):
    player_ids_list = []
    for year in years: 
        qualified_players = batting_stats(year, qual=qual)
        player_ids = playerid_reverse_lookup(qualified_players['IDfg'].values, key_type='fangraphs')
        player_ids = player_ids['key_mlbam'].values.tolist()
        player_ids_list.extend(player_ids)
        player_ids_list = list(dict.fromkeys(player_ids_list))
    return player_ids_list
    
# if __name__ == "__main__":
#     arg1_value = [2022, 2021]
#     arg2_value = 300
#     result = get_qualified_players(arg1_value, arg2_value)
#     print(result)