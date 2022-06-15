from datetime import datetime
import sqlite3

def log(message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y'
    #Hour-Minute-Second-MonthName-Day-Year
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("etl_logfile.txt","a") as f:
        f.write(f'{timestamp},{message}/n')

# def create_connection(db_name):
#     """ create a database connection to the SQLite database
#         specified by db_file
#     :param db_file: database file
#     :return: Connection object or None
#     """
#     conn = None
#     try:
#         conn = sqlite3.connect(db_name)
#         return conn
#     except Error as e:
#         print(e)

#     return conn

# def create_table(conn, create_table_sql):
#     """ create a table from the create_table_sql statement
#     :param conn: Connection object
#     :param create_table_sql: a CREATE TABLE statement
#     :return:
#     """
#     try:
#         c = conn.cursor()
#         c.execute(create_table_sql)
#     except Error as e:
#         print(e)

# def main(db_name):
#     database = 'variant.db'

#     # create a database connection
#     sql_create_projects_table = """ CREATE TABLE IF NOT EXISTS projects (
#                                         CHROM text,
#                                         POS integer,
#                                         ID text,
#                                         REF text,
#                                         ALT text,
#                                         Gene text,
#                                         VC text,
#                                         Accession text
#                                     ); """

#     conn = create_connection(database)

#     # create tables
#     if conn is not None:
#         # create projects table
#         create_table(conn, sql_create_projects_table)

#     else:
#         print("Error! cannot create the database connection.")

def create_db(db_name, annotated_df):
    
    conn = None
    try:
        conn = sqlite3.connect(db_name)
        return conn
    except Error as e:
        print(e)

    sql_create_variants_table = """ CREATE TABLE IF NOT EXISTS variants (
                                        CHROM text,
                                        POS integer,
                                        ID text,
                                        REF text,
                                        ALT text,
                                        Gene text,
                                        VC text,
                                        Accession text
                                    ); """

    try:
        c = conn.cursor()
        c.execute(sql_create_variants_table)
    except Error as e:
        print(e)

    annotated_df.to_sql('variants', conn, if_exists='replace' )




