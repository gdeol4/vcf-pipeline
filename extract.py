import pandas as pd
import pysam

VCF_DATA = 'GRCh38_latest_dbSNP_all.vcf.gz'

def vcf_slicer(chromosome, start, end) -> pd.DataFrame:
    """_summary_

    Args:
        chromosome (_type_): _description_
        start (_type_): _description_
        end (_type_): _description_

    Returns:
        _type_: _description_
    """

    tbx = pysam.TabixFile(VCF_DATA)

    chromosomes = {
        1: "NC_000001.11",
        2: "NC_000002.12",
        3: "NC_000003.12",
        4: "NC_000004.12",
        5: "NC_000005.10",
        6: "NC_000006.12",
        7: "NC_000007.14",
        8: "NC_000008.11",
        9: "NC_000009.12",
        10: "NC_000010.11",
    }

    chromosome = chromosomes[chromosome]

    chromos = list(tbx.fetch(chromosome, start, end, parser=pysam.asTuple()))

    return pd.DataFrame(chromos)

def extract_vcf(chromo_range):
    """_summary_

    Args:
        chromo_range (_type_): _description_

    Returns:
        _type_: _description_
    """
    
    chr_list = [vcf_slicer(i, 0, 50000) for i in chromo_range]

    df = pd.concat(chr_list)
    df = df.drop(df.iloc[:, [5, 6]], axis=1)
    df.columns = ["CHROM", "POS", "ID", "REF", "ALT", "INFO"]
    df["ALT"] = df["ALT"].str.split(",")
    df = df.explode("ALT")
    df["INFO"] = df["INFO"].astype("string")

    return df

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

