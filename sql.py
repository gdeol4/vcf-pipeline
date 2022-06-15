import streamlit as st
import sqlite3









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