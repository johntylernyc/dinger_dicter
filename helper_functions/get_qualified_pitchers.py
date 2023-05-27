from pybaseball import pitching_stats
from pybaseball import playerid_reverse_lookup


def get_qualified_pitchers(years, qual):
    player_ids_list = []
    for year in years: 
        try:
            qualified_pitchers = pitching_stats(year, qual=qual)
            print(f'qualified_pitchers data for {year}:\n{qualified_pitchers.head()}')
        except ValueError as ve:
            print(f'Error occurred when fetching pitching stats for {year}: {ve}')
            continue

        player_ids = playerid_reverse_lookup(qualified_pitchers['IDfg'].values, key_type='fangraphs')
        player_ids = player_ids['key_mlbam'].values.tolist()
        player_ids_list.extend(player_ids)
        player_ids_list = list(dict.fromkeys(player_ids_list))

        # limit player_ids_list to 3 for testing purposes
        #TODO: Remove test case when finished testing
        player_ids_list = player_ids_list[:3]

    return player_ids_list


if __name__ == "__main__":
    arg1_value = [2022, 2021]
    arg2_value = 100
    result = get_qualified_pitchers(arg1_value, arg2_value)
    print(result)
    #print type of object result is 
    print(type(result))