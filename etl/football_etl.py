import constants as c
import func as f
import pandas as pd


def run_football_initial_etl(etl_range:int, df: pd.DataFrame, verbose:bool=True):
    if verbose:
        print('\nStart football ETL process.')
        print('\nGet statistics from matches.\n')

    match_to_skip = None
    for _ in range(etl_range):
        match_id = f.get_last_match_id(df, match_to_skip)
        match_to_skip = None

        if not match_id:
            print('\nFile should be full. If not, check "get_last_match_id" function.\n')
            return

        if verbose:
            print(f'\nGet match num {match_id}')

        try:
            stats_info = f.get_stats_info(match_id)
            f.save_stats_info_in_df(df, match_id, stats_info)
        except Exception as err:
            match_to_skip = match_id
            if verbose:
                print(f'\nSomething went wrong with match number {match_id}!\n')
                print(err)

    if verbose:            
        print('\nThe data was successfully downloaded\n')


if __name__ == '__main__':

    etl_range = 95
    csv_name = c.CSV_NAME

    df = f.read_csv_from_s3(csv_name)

    run_football_initial_etl(etl_range, df)
    
    f.write_csv_into_s3(csv_name, df)
