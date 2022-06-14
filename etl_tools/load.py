from datetime import datetime
import sqlite3

def log(message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y'
    #Hour-Minute-Second-MonthName-Day-Year
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("etl_logfile.txt","a") as f:
        f.write(f'{timestamp},{message}/n')

def create_connection(db_name):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_name)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
    return print('made')