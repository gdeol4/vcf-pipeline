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
        11: "NC_000011.10",
        12: "NC_000012.12",
        13: "NC_000013.11",
        14: "NC_000014.9",
        15: "NC_000015.10",
        16: "NC_000016.10",
        17: "NC_000017.11",
        18: "NC_000018.10",
        19: "NC_000019.10",
        20: "NC_000020.11",
        21: "NC_000021.9",
        22: "NC_000022.11",
        'X': "NC_000023.11",
        'Y': "NC_000024.10",
    }

    chromosome = chromosomes[chromosome]
    chromos = list(tbx.fetch(chromosome, start, end, parser=pysam.asTuple()))
    return pd.DataFrame(chromos)

def slice_format(df):
    df.columns = ['Chromosome', 'Position', 'ID', 'REF', 'ALT', 'Strand', 'Phase', 'INFO']
    return df

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

