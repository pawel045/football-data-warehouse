# Functions for connecting with API and corresponding with S3

import boto3
from datetime import datetime, timedelta
from my_secret_key import *
import pandas as pd
import requests

pd.set_option('mode.chained_assignment', None)


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
            save_match_info_in_ds(data_dict, matches)
        except Exception as err:
            if verbose:
                print(f'Something went wrong during season {season}:')
            print(err)
        
        if verbose:
            print(f'Season {season} done!\n')
    
    if verbose:
        print(f'\nAll data downloaded correctly!\nThe job is done.\n')

    fullfill_minusone_ds(data_dict)
    df = pd.DataFrame(data_dict)
    df.to_csv(c.CSV_NAME)


def get_season_info(league_id, year):
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"

    headers = {
        "X-RapidAPI-Key": SECRET_KEY,
        "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
    }

    params = {
        "league": league_id,
        "season": year
    }

    response = requests.get(url, headers=headers, params=params).json()
    matches = response["response"]
    return matches


def set_as_int(data):
    try:
        results = int(data)
    except TypeError:
        results = -1
    
    return results


def save_match_info_in_ds(data_dict: dict, matches: list):
    for match in matches:
        # match
        data_dict['match_id'].append(match['fixture']['id'])
        data_dict['date'].append(match['fixture']['date'])
        data_dict['country'].append(match['league']['country'])
        data_dict['league'].append(match['league']['name'])
        data_dict['venue'].append(match['fixture']['venue']['name'])
        data_dict['season'].append(match['league']['season'])
        data_dict['round'].append(match['league']['round'])

        # winner/loser
        home_score = set_as_int(match['score']['fulltime']['home'])
        away_score = set_as_int(match['score']['fulltime']['away'])

        if home_score == -1 or away_score == -1:
            who_win = 'err'
        elif home_score > away_score:
            who_win = 'home'
        elif home_score < away_score:
            who_win = 'away'
        elif home_score == away_score:
            who_win = 'draw'

        data_dict['who_win'].append(who_win)

        # home
        data_dict['home'].append(match['teams']['home']['name'])
        data_dict['home_goals_ht'].append(set_as_int(match['score']['halftime']['home']))
        data_dict['home_goals_ft'].append(home_score)

        # away
        data_dict['away'].append(match['teams']['away']['name'])
        data_dict['away_goals_ht'].append(set_as_int(match['score']['halftime']['away']))
        data_dict['away_goals_ft'].append(away_score)


def fullfill_minusone_ds(data_dict: dict):
    len_items = len(data_dict['match_id'])
    for key, val in data_dict.items():
        if len(val) == 0:
            for i in range(len_items):
                data_dict[key].append(-1)


def get_last_match_id(df: pd.DataFrame):
    for index, row in df.iterrows():
        if row['home_shots_on_goal'] == -1:
            return row['match_id']
    return False


def get_stats_info(match_id):
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"

    querystring = {"fixture":match_id}

    headers = {
        "X-RapidAPI-Key": SECRET_KEY,
        "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    return response.json()['response']


def save_stats_info_in_df(df, match_id, stats):
    # home
    home_list = ['home_shots_on_goal', 'home_shots_off_goal', 'home_total_shots', 'home_blocked_shots',
                 'home_shots_insidebox', 'home_shots_outsidebox', 'home_fouls', 'home_corner_kick',
                 'home_offsides', 'home_ball_possession', 'home_yellow_cards', 'home_red_cards', 
                 'home_goalkeeper_saves', 'home_total_passes', 'home_passes_accurate', 'home_passes_percent']
    # away
    away_list = ['away_shots_on_goal', 'away_shots_off_goal', 'away_total_shots', 'away_blocked_shots',
                 'away_shots_insidebox', 'away_shots_outsidebox', 'away_fouls', 'away_corner_kick',
                 'away_offsides', 'away_ball_possession', 'away_yellow_cards', 'away_red_cards', 
                 'away_goalkeeper_saves', 'away_total_passes', 'away_passes_accurate', 'away_passes_percent']

    for i, stat in enumerate(home_list):
        df[stat].loc[df['match_id']==match_id] = stats[0]['statistics'][i]['value']

    for i, stat in enumerate(away_list):
        df[stat].loc[df['match_id']==match_id] = stats[1]['statistics'][i]['value']


def read_csv_from_s3(name: str):
    s3_client = boto3.client('s3')
    resposne = s3_client.get_object(Bucket=BUCKET_NAME, Key=name)

    df = pd.read_csv(resposne.get("Body"))
    return df


def write_csv_into_s3(name: str, df: pd.DataFrame):

    df.to_csv(f"s3://{BUCKET_NAME}/{name}")


def n_days_ago(n: int):
    now = datetime.today()
    delay = timedelta(days=n)
    n_days_ago = now - delay

    return n_days_ago.strftime('%Y-%m-%d')


def get_match_ids_from_last_three_days():
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    today = n_days_ago(0)
    four_days_ago = n_days_ago(4)
    year = today.year

    querystring = {"league":"39", "season":"2023", "from":four_days_ago, "to":today}
    headers = {
        "X-RapidAPI-Key": "593dec4a73msh0b59f1a2ed01ab0p190b44jsnd1a3034279ce",
        "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    print(response.json())


