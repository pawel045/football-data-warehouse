# This script is use for uploading all already collected data to a database.
# Use it only once.

import custom_functions.func as f
import custom_functions.postgres_func as pf
import pandas as pd


df = pd.read_csv('football_data.csv')


with pf.create_connection() as conn:

    # replace na to 'None'
    df.fillna(-1, inplace=True)

    # create league and clubs table
    for i, row in df.iterrows():
        league_name = row['league']
        country = row['country']
        pf.create_league(conn, league_name, country)

        club_name = row['home']
        stadion_name = row['venue']
        pf.create_club(conn, club_name, stadion_name)

    # add rows to match_stats table
    for i, row in df.iterrows():
        
        print(f'\nCreate row with id {i}...')
        match_id = pf.create_match_id(conn)

        league_id = pf.get_id(conn, 'league', row['league'])
        home_id = pf.get_id(conn, 'clubs', row['home'])
        away_id = pf.get_id(conn, 'clubs', row['away'])
        

        values = (
            match_id,
            league_id,
            home_id,
            away_id,
            row['who_win'],
            row['home_goals_ht'],
            row['home_goals_ft'],
            row['home_shots_on_goal'],
            row['home_shots_off_goal'],
            row['home_total_shots'],
            row['home_blocked_shots'],
            row['home_shots_insidebox'],
            row['home_shots_outsidebox'],
            row['home_fouls'],
            row['home_corner_kick'],
            row['home_offsides'],
            f.percentage_to_float(row['home_ball_possession']),
            row['home_yellow_cards'],
            row['home_red_cards'],
            row['home_goalkeeper_saves'],
            row['home_total_passes'],
            row['home_passes_accurate'],
            f.percentage_to_float(row['home_passes_percent']),
            row['away_goals_ht'],
            row['away_goals_ft'],
            row['away_shots_on_goal'],
            row['away_shots_off_goal'],
            row['away_total_shots'],
            row['away_blocked_shots'],
            row['away_shots_insidebox'],
            row['away_shots_outsidebox'],
            row['away_fouls'],
            row['away_corner_kick'],
            row['away_offsides'],
            f.percentage_to_float(row['away_ball_possession']),
            row['away_yellow_cards'],
            row['away_red_cards'],
            row['away_goalkeeper_saves'],
            row['away_total_passes'],
            row['away_passes_accurate'],
            f.percentage_to_float(row['away_passes_percent']),
            row['date']
        )

        pf.insert_to_match_stats(conn, values)

    print('\nAll data uploaded\n')