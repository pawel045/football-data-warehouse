# ETL process for last round in Premier League.

import constants as c
import custom_functions.func as f
import pandas as pd
import custom_functions.postgres_func as pf

print('\nStart ETL process...\n')

# create connection with DB
conn = pf.create_connection()

# ============================
# EXTRACT DATA FROM LAST ROUND
# ============================
print('Extracting data from API...')
try:
    match_ids_entangled = f.get_match_ids_from_last_three_days('Premier League')
except Exception as err:
    raise err
print('Extracting completed!\n')

for match in match_ids_entangled:
    # ===============================================
    # TRANSFORM SINGLE MATCH TO SUITABLE WITH DB FORM
    # ===============================================
    id = match['fixture']['id']
    match_stats = f.get_stats_info(id)

    if match['teams']['home']['winner'] == 'True':
        who_win = 'home'
    elif match['teams']['home']['winner'] == 'False':
        who_win = 'away'
    else:
        who_win = 'draw'

    info = {    
        'id': pf.create_match_id(conn),
        'date': match['fixture']['date'],
        'country': match['league']['country'],
        'league': match['league']['name'],
        'venue': match['fixture']['venue']['name'],
        'season': match['league']['season'],
        'round': match['league']['round'],
        'who_win': who_win,

        'home': match['teams']['home']['name'],
        'home_goals_ht': match['score']['halftime']['home'],
        'home_goals_ft': match['score']['fulltime']['home'],

        'away': match['teams']['away']['name'],
        'away_goals_ht': match['score']['halftime']['away'],
        'away_goals_ft': match['score']['fulltime']['away'],
    }

    for key, val in info.items():
        if not val:
            info[key] = -1

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
        val = match_stats[0]['statistics'][i]['value']
        if val:
            info[stat] = val
        else:
            info[stat] = -1

    for i, stat in enumerate(away_list):
        val = match_stats[1]['statistics'][i]['value']
        if val:
            info[stat] = val
        else:
            info[stat] = -1
    
    # ==========
    # LOAD TO DB
    # ==========

    print('Creating a league and club...')
    # create league and clubs table
    league_name = info['league']
    country = info['country']
    pf.create_league(conn, league_name, country)

    club_name = info['home']
    stadion_name = info['venue']
    pf.create_club(conn, club_name, stadion_name)

    print('Creating completed./n')

    # add row to match_stats table
    print(f'\nCreate stats row...')
    match_id = pf.create_match_id(conn)

    league_id = pf.get_id(conn, 'league', info['league'])
    home_id = pf.get_id(conn, 'clubs', info['home'])
    away_id = pf.get_id(conn, 'clubs', info['away'])

    values = (
            match_id,
            league_id,
            home_id,
            away_id,
            info['who_win'],
            info['home_goals_ht'],
            info['home_goals_ft'],
            info['home_shots_on_goal'],
            info['home_shots_off_goal'],
            info['home_total_shots'],
            info['home_blocked_shots'],
            info['home_shots_insidebox'],
            info['home_shots_outsidebox'],
            info['home_fouls'],
            info['home_corner_kick'],
            info['home_offsides'],
            f.percentage_to_float(info['home_ball_possession']),
            info['home_yellow_cards'],
            info['home_red_cards'],
            info['home_goalkeeper_saves'],
            info['home_total_passes'],
            info['home_passes_accurate'],
            f.percentage_to_float(info['home_passes_percent']),
            info['away_goals_ht'],
            info['away_goals_ft'],
            info['away_shots_on_goal'],
            info['away_shots_off_goal'],
            info['away_total_shots'],
            info['away_blocked_shots'],
            info['away_shots_insidebox'],
            info['away_shots_outsidebox'],
            info['away_fouls'],
            info['away_corner_kick'],
            info['away_offsides'],
            f.percentage_to_float(info['away_ball_possession']),
            info['away_yellow_cards'],
            info['away_red_cards'],
            info['away_goalkeeper_saves'],
            info['away_total_passes'],
            info['away_passes_accurate'],
            f.percentage_to_float(info['away_passes_percent']),
            info['date']
        )
    
    try:
        pf.insert_to_match_stats(conn, values)
    except Exception:
        print("Can't add stats row. This row is skipped.\n")


print('ETL process end.\n')

conn.close()