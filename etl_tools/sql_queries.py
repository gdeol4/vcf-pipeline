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
    q1.columns = ['Chromosome', 'Variant count']

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
    q2.columns = ['Chromosome', 'ID', 'REF', 'ALT', 'Gene', 'Variant', 'Start', 'Stop', 'Accession', 'GeneID', 'start_y', 'Chromosome_y']
    q2 = q2.drop(['ID', 'Stop', 'Accession', 'GeneID', 'start_y', 'Chromosome_y'], axis = 1)
    conn.close()
    return q2


def query_3(db_name):

    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    results = c.execute(
        '''SELECT CHROM, VC, COUNT(CHROM) FROM variant GROUP BY CHROM, VC;''')
    q3 = pd.DataFrame(results.fetchall())
    q3.columns = ['Chromosome', 'Variant', 'Count']
    conn.close()
    return q3


def query_4(db_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    results = c.execute('''SELECT VC, COUNT(*)
                           FROM variant
                           GROUP BY VC
                           ORDER BY COUNT(*)
                           DESC LIMIT 1''')
    q4 = pd.DataFrame(results.fetchall())
    q4.columns = ['Variant type', 'Variant count']
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
    q5.columns = ['Gene', 'Variant count']
    conn.close()
    return q5
