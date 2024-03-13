def create_connection(db_path):
    '''
    Create connecition to database;
        :param db_path: full path to specifi database    
        :return: connection object or None
    '''
    conn = None
    
    try:
        conn = sqlite3.connect(db_path)   
    except Error as err:
        print(err)
    
    return conn


def create_table(conn, table_sql):
    '''
    Create tabel using SQL statment to database
        :param conn: connection object
        :param table_sql: create table as SQL code
        :return:
    '''
    try:
        cursor = conn.cursor()
        cursor.execute(table_sql)
        print('Profile table created!')
    except Error as err:
        print(err)   


def create_profile(conn, profile):
    '''
    Create a new profile into profiles table
        :param conn: connection object
        :param profile: new profile data
        :return:
    '''
    sql = ''' INSERT INTO profiles(is_30,pesel,post_code,prop_type,area,prop_val,prop_theft,move_prop_val,move_prop_theft,query_date,company,product,total_price,oc_priv)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''

    cursor = conn.cursor()
    cursor.execute(sql, profile)
    conn.commit()
    print(f'New profile created -> id: {cursor.lastrowid}')


def delete_profile(conn, profile_id):
    '''
    Delete profile selected by id
        :param conn: connection object
        :param profile_id: profile to delete
        :return:
    '''
    sql = 'DELETE FROM profiles WHERE id=?'
    
    try:
        cursor = conn.cursor()
        cursor.execute(sql, str(profile_id))
        conn.commit()
        print(f'Profile deleted -> id: {str(profile_id)}')
    except Error as err:
        print(err)


def delete_all_profiles(conn):
    '''
    Delete all profiles 
        :param conn: connection object
        :return:
    '''
    sql = 'DELETE FROM profiles'
    
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        print(f'All profiles deleted!')
    except Error as err:
        print(err)


def len_table(conn):
    '''
    Check length of table
        :param conn: connection object
        :return:
    '''
    cursor = conn.cursor()
    cursor.execute('select * from profiles')
    results = cursor.fetchall()
    print(f'Length of profiles table: {len(results)}')