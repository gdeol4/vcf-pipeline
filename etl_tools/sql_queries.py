import sqlite3
import pandas as pd

def query_1(db_name):

    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    results = c.execute('''SELECT CHROM, COUNT(*) 
                           FROM variant 
                           GROUP BY CHROM 
                           ORDER BY COUNT(*) 
                           DESC LIMIT 1''')
    q1 = pd.DataFrame(results.fetchall())
    conn.close()

    return q1

def query_2(db_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    sub_query = '''SELECT CHROM, MIN(start) min_start 
                   FROM variant 
                   GROUP BY CHROM'''
    query = f'''SELECT * 
                FROM variant a 
                INNER JOIN ({sub_query}) b ON a.CHROM = b.CHROM AND a.start = b.min_start'''
    results = c.execute(query)
    q2 = pd.DataFrame(results.fetchall())
    conn.close()
    return q2

def query_4(db_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    results = c.execute('''SELECT VC, COUNT(*) 
                           FROM variant 
                           GROUP BY VC 
                           ORDER BY COUNT(*) 
                           DESC LIMIT 1''')
    q4 = pd.DataFrame(results.fetchall())
    conn.close()
    return q4

def query_5(db_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    results = c.execute('''SELECT Gene, COUNT(*) 
                         FROM variant 
                         GROUP BY Gene 
                         ORDER BY COUNT(*) DESC''')
    q5 = pd.DataFrame(results.fetchall())
    conn.close()
    return q5
