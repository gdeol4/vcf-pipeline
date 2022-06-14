import pandas as pd


def extract_gff3(filename):
    """_summary_
    Args:
        filename (_type_): _description_
    Returns:
        _type_: _description_
    """    
    col_names = [
    "seqid",
    "source",
    "type",
    "start",
    "end",
    "score",
    "strand",
    "phase",
    "attributes",
    ]

    return pd.read_csv('GRCh38_latest_genomic.gff.gz', 
                        compression='gzip', sep='\t', 
                        comment='#', low_memory=False, 
                        header=None, 
                        names=col_names)