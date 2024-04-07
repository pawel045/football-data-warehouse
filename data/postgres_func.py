# Function for connecting with Postgres

import psycopg2
from my_secret_key import POSTGRES_PASSWORD, POSTGRES_DATABASE_NAME, POSTGRES_USER


def create_connection(db_name=POSTGRES_DATABASE_NAME, user=POSTGRES_USER, password=POSTGRES_PASSWORD):
    '''
    Create connecition to database;
        :param db_name: name of database to connect
        :param: name of Postgres owner
        :return: connection object or None
    '''
    conn = None

    try:
        conn = psycopg2.connect(f'dbname={db_name} user={user} password={password}')
    except Exception as err:
        print(err)
    
    return conn


def is_value_exists(conn, table_name, column_name, value):
    '''
    Function checks if value exists in specific column in table
    :param conn: name of database to connect
    :param table_name: str
    :param column_name: str
    :param value: 
    :return: bool, if False row value doesn't exist
    '''

    cursor = conn.cursor()
    sql_check_if_exist = f"select count(*) from football.{table_name} where {column_name}='{value}'"
    
    cursor.execute(sql_check_if_exist)
    records = cursor.fetchall()

    if records[0][0] == 0:
        return False
    else:
        return True


def create_league(conn, league_name:str, country:str):
    '''
    Create a new league in database
        :param conn: connection object
        :param club_name: name of new league
        :param stadion_name: name of league's country
    '''

    sql = f'''INSERT INTO football.league (league_name, league_country)
              VALUES ('{league_name}', '{country}');'''
    
    if is_value_exists(conn, 'league', 'league_name', league_name):
        print(f'{league_name.title()} already exists in database.')
        return

    cursor = conn.cursor()
    cursor.execute(sql)

    conn.commit()
    print(f'New profile created -> id: {cursor.lastrowid}')


def create_club(conn, club_name:str, stadion_name:str):
    '''
    Create a new club in database
        :param conn: connection object
        :param club_name: name of new club
        :param stadion_name: name of new clubs's stadion
    '''

    sql = f'''INSERT INTO football.clubs (club_name, club_stadion)
              VALUES ('{club_name}', '{stadion_name}');'''
    
    if is_value_exists(conn, 'clubs', 'club_name', club_name):
        print(f'{club_name.title()} already exists in database.')
        return

    cursor = conn.cursor()
    cursor.execute(sql)

    conn.commit()
    print(f'New profile created -> id: {cursor.lastrowid}')


def get_id(conn, table, value):
    '''
    Get a number of id from clubs or league table
    :param conn: connection object
    :param table: name of particular table
    :param value: value for WHERE clause
    :return records[0][0]: number of id from first (and only) row results
    '''
    if table == 'clubs':
        column = 'club'
    elif table == 'league':
        column = 'league'
    else:
        print('Wrong table name. Only two options: "clubs" or "league"')
        return

    sql = f"SELECT {column}_id FROM football.{table} WHERE {column}_name = '{value}'"

    cursor = conn.cursor()
    cursor.execute(sql)
    records = cursor.fetchall()

    return records[0][0]


def create_match_id(conn):
    '''
    Create a integer id, greater by 1 than the biggest number in table.
    :param conn: connection object
    :return new_id: new id for table
    '''

    sql = f"SELECT match_id FROM football.match_stats ORDER BY 1 desc LIMIT 1;"

    cursor = conn.cursor()
    cursor.execute(sql)
    
    try:
        old_id = int(cursor.fetchall()[0][0])
        new_id = old_id + 1
    except:
        new_id = 1

    return new_id


def insert_to_match_stats(conn, values):
    sql = f'''INSERT INTO football.match_stats VALUES {values}'''
    
    cursor = conn.cursor()
    cursor.execute(sql)

    print(f'New row added')
    conn.commit()


def percentage_to_float(percentage:str):
    try:
        number = int(percentage.replace('%', ''))
        result = round(float(number/100), 2)
    except Exception as err:
        print(err)

    return result


# with create_connection() as conn:
#     get_id(conn, 'league', )

# # Connect to your postgres DB
# conn = psycopg2.connect(f"dbname=football_db user=postgres password={POSTGRES_PASSWORD}")
# print(type(conn))

# # Open a cursor to perform database operations
# cur = conn.cursor()

# # Execute a query
# sql_check_if_exist = f"select count(*) from football.clubs where club_name='test1'"
# cur.execute(sql_check_if_exist)

# # Retrieve query results
# records = cur.fetchall()

# print(records)

# if records[0][0] == 1:
#     print('TAK')
# else:
#     print('NIE')
