import streamlit as st
import sqlite3
import pandas as pd


annotated_df = pd.read_csv('out.csv')
st.write(annotated_df.head())

db_name = 'variants.db'


# def create_db(db_name, annotated_df):
    
#     conn = None
#     try:
#         conn = sqlite3.connect(db_name)
#         return conn
#     except Error as e:
#         print(e)

#     sql_create_variants_table = """ CREATE TABLE IF NOT EXISTS variants (
#                                         CHROM text,
#                                         POS integer,
#                                         ID text,
#                                         REF text,
#                                         ALT text,
#                                         Gene text,
#                                         VC text,
#                                         Accession text
#                                     ); """

#     try:
#         c = conn.cursor()
#         c.execute(sql_create_variants_table)
#     except Error as e:
#         print(e)

#     annotated_df.to_sql('variants', conn, if_exists='replace' )


def create_db(db_name, annotated_df):
    

    conn = sqlite3.connect(db_name)

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

    c = conn.cursor()
    c.execute(sql_create_variants_table)

    annotated_df.to_sql('variants', conn, if_exists='replace' )

    results = c.execute('SELECT DISTINCT Gene FROM variants')

    test_q = pd.DataFrame(results.fetchall())

    conn.close()

    return test_q

#st.write(create_db('test.db', annotated_df))

def query_1(db_name):

    conn = sqlite3.connect(db_name)

    c = conn.cursor()

    results = c.execute('SELECT CHROM, COUNT(*) FROM variants GROUP BY CHROM ORDER BY COUNT(*) DESC LIMIT 1')

    q1 = pd.DataFrame(results.fetchall())

    conn.close()

    return q1

def query_2(db_name):

    conn = sqlite3.connect(db_name)

    c = conn.cursor()

    sub_command = "SELECT CHROM, MIN(start) min_start FROM variants GROUP BY CHROM"
    command = f"SELECT * FROM variants a INNER JOIN ({sub_command}) b ON a.CHROM = b.CHROM AND a.start = b.min_start"

    results = c.execute(command)

    q2 = pd.DataFrame(results.fetchall())

    conn.close()

    return q2

def query_4(db_name):

    conn = sqlite3.connect(db_name)

    c = conn.cursor()

    results = c.execute('SELECT Gene, COUNT(*) FROM variants GROUP BY Gene ORDER BY COUNT(*) DESC')

    q4 = pd.DataFrame(results.fetchall())

    conn.close()

    return q4

if st.button('Run db'):
    st.write(create_db(db_name, annotated_df))

if st.button('Query 1'):
    st.write(query_1(db_name))

if st.button('Query 2'):
    st.write(query_2(db_name))

if st.button('Query 4'):
    st.write(query_4(db_name))