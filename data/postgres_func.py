import psycopg2
from my_secret_key import POSTGRES_PASSWORD, POSTGRES_DATABASE_NAME, POSTGRES_USER


def create_connection(db_name=POSTGRES_DATABASE_NAME, user=POSTGRES_USER):
    '''
    Create connecition to database;
        :param db_name: name of database to connect
        :param: name of Postgres owner
        :return: connection object or None
    '''
    conn = None

    try:
        conn = psycopg2.connect(f'dbname={db_name} user={POSTGRES_USER} password={POSTGRES_PASSWORD}')
    except Exception as err:
        print(err)
    
    return conn


# select distinct club_name from football.clubs
# where club_name = 'test1'


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



with create_connection() as conn:
    create_club(conn, 'test1', 'test1')

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
