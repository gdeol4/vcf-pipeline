import pysam
import pandas as pd

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


def combine_chromos(chromo_range):
    """_summary_

    Args:
        chromo_range (_type_): _description_

    Returns:
        _type_: _description_
    """
    chr_list = [vcf_slicer(i, 0, 50000) for i in chromo_range]

    chr_list = pd.concat(chr_list)
    chr_list = chr_list.drop(chr_list.iloc[:, [5, 6]], axis=1)
    chr_list.columns = ["CHROM", "POS", "ID", "REF", "ALT", "INFO"]
    chr_list["ALT"] = chr_list["ALT"].str.split(",")
    chr_list = chr_list.explode("ALT")
    chr_list["INFO"] = chr_list["INFO"].astype("string")

    return chr_list

