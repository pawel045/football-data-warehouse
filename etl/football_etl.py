import constants as c
import func as f
import os
import pandas as pd
import numpy as np

def create_initial_csv(seasons: list, league_id:int, verbose:bool=True):
    data_dict = c.DATA_STRUCTURE
    seasons.sort()
    
    if verbose:
        print(f'\nStart downloading data for seasons: {seasons[0]}-{seasons[-1]}...\n\n')

    for season in seasons:

        if verbose:
            print(f'Get season {season}...')

        try:
            matches = f.get_season_info(league_id, season)
            f.save_match_info_in_ds(data_dict, matches)
        except Exception as err:
            if verbose:
                print(f'Something went wrong during season {season}:')
            print(err)
        
        if verbose:
            print(f'Season {season} done!\n')
    
    if verbose:
        print(f'\nAll data downloaded correctly!\nThe job is done.\n')

    f.fullfill_minusone_ds(data_dict)
    df = pd.DataFrame(data_dict)
    df.to_csv(c.CSV_NAME)


def run_football_etl(seasons: list, league_id:int, verbose:bool=True):
    if verbose:
        print('\nStart football ETL process.')
        print('\nCheck if exists initial csv file.')

    # check if exists initial csv
    curr_path = os.getcwd()
    if not os.path.exists(curr_path + '/' + c.CSV_NAME):
        create_initial_csv(seasons, league_id, verbose)
    else:
        if verbose:
            print('The initial CSV file has already been created.\n')

    if verbose:
        print('\nGet statistics from matches.\n')

    # fill stats info
    df = pd.read_csv(c.CSV_NAME)

    for i in range(3):
        match_id = f.get_last_match_id(df)

        if verbose:
            print(f'\nGet match num {match_id}')

        try:
            stats_info = f.get_stats_info(match_id)
            f.save_stats_info_in_df(df, match_id, stats_info)
        except Exception as err:
            if verbose:
                print('\nSomething went wrong with match number {match_id}!\n')
                print(err)

    df.to_csv(c.CSV_NAME)

# season_year = "2020"
# league_id = c.PREMIER_LEAGUE_ID

# data_dict = c.DATA_STRUCTURE
# matches = f.get_season_info(league_id, season_year)

# f.save_match_info_in_ds(data_dict, matches)
# f.fullfill_minusone_ds(data_dict)

# df = pd.DataFrame(data=data_dict)
# df.to_csv('./tmp_csv/test.csv')

# df = f.extract_csv_as_df('test')
# match_id = f.get_last_match_id(df)

# res = f.get_stats_info(match_id)
# f.save_stats_info_in_df(df, 592143, res)

# df.to_csv('tmp_csv/test_with_stats.csv')


if __name__ == '__main__':
    seasons = [2019, 2020, 2021, 2022, 2023]
    league_id = c.PREMIER_LEAGUE_ID
    run_football_etl(seasons, league_id)

