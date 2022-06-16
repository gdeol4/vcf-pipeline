import sqlite3
import pandas as pd
import urllib
from io import StringIO


def create_db(db_name, annotated_df):

    conn = sqlite3.connect(db_name)

    sql_create_variants_table = """ CREATE TABLE IF NOT EXISTS variant (
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
    annotated_df.to_sql('variant', conn, if_exists='replace', index=False)
    conn.close()

    return None


def get_uniprot_sequences(gene_list) -> pd.DataFrame:
    """
    Retrieve uniprot sequences based on a list of uniprot sequence identifier.

    For large lists it is recommended to perform batch retrieval.

    documentation which columns are available:
    https://www.uniprot.org/help/uniprotkb%5Fcolumn%5Fnames

    this python script is based on
    https://www.biostars.org/p/67822/

    Parameters:
        uniprot_ids: List, list of uniprot identifier

    Returns:
        pd.DataFrame, pandas dataframe with uniprot id column and sequence
    """

    # This is the webserver to retrieve the Uniprot data
    url = 'https://www.uniprot.org/uploadlists/'
    params = {
        'from': "GENENAME",
        'to': 'ACC',
        'format': 'tab',
        'query': " ".join(gene_list),
        'columns': 'id,sequence'}

    data = urllib.parse.urlencode(params)
    data = data.encode('ascii')
    request = urllib.request.Request(url, data)
    with urllib.request.urlopen(request) as response:
        res = response.read()
    df_fasta = pd.read_csv(StringIO(res.decode("utf-8")), sep="\t")
    df_fasta.columns = ["Entry", "Sequence", "Query"]
    # it might happen that 2 different ids for a single query id are returned,
    # split these rows
    return df_fasta.assign(
        Query=df_fasta['Query'].str.split(',')).explode('Query')


def process_uniprot(df):
    df = df.drop_duplicates(subset='Query', keep='first')
    df = df[["Query", "Entry", "Sequence"]]
    df.rename(columns={'Query': 'Gene', 'Entry': 'Uniprot ID'}, inplace=True)

    return df
