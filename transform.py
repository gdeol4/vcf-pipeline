import pandas as pd
import re

def vc_extract(info, df):
    """_summary_
    Args:
        info (_type_): _description_
        df (_type_): _description_
    Returns:
        _type_: _description_
    """

    vcs = []
    for variant in info:
        # search for VC tag
        line = variant.split(";")
        found_vc = False
        for tag in line:
            # if VC is found add it to gene
            if "VC" in tag:
                vcs.append(tag.replace("VC=", "").split(","))
                found_vc = True
        # if not found add empty row
    if not found_vc:
        vcs.append([])

    df["VC"] = pd.DataFrame(vcs)

    return df

def extract_attributes(df):
    
    df['Parent'] = df.attributes.apply(extract_parent)
    df['Dbxref'] = df.attributes.apply(extract_Dbxref)
    df['GeneID'] = df.Dbxref.apply(extract_GeneID)
    df['Genbank'] = df.Dbxref.apply(extract_Genbank)
    df = df[df['seqid'].isin(chromosomes)]
    df = df[df['start'].between(0, 50000)]
    
    return df

def process_attributes(df):
    
    ndf = df[(df.type == 'gene') | (df.type == 'pseudogene')]
    ndf['Gene'] = df.attributes.apply(extract_gene)
    ndf = ndf[ndf['seqid'].isin(chromosomes)]
    ndf['GeneID'] = df['GeneID'].replace('', pd.NA).fillna(df['Dbxref'])
    ndf["GeneID"] = ndf["GeneID"].str.replace(r'^[^:]*,*:', '', regex=True)
    ndf = ndf.drop(['score', 'phase', 'attributes', 'Dbxref', 'source'], axis = 1)
    df = df.drop(['score', 'phase', 'attributes', 'Dbxref', 'source'], axis = 1)

    return ndf

def find_genbank(df):
    
    df3 = pd.merge(ndf, df[['GeneID','Genbank', 'Parent']],on='GeneID', how='left')
    ndf = ndf.drop(['Parent', 'Genbank'], axis = 1)
    df3 = df3.drop(['Parent_x', 'Genbank_x'], axis = 1)
    df3.rename(columns = {'Parent_y':'Parent', 'Genbank_y':'Genbank'}, inplace = True)
    
    return df3

def remove_duplicates(df):
    
    nan_value = float("NaN")
    df3.replace("", nan_value, inplace=True)
    df3 = df3.drop_duplicates(subset=['seqid', 'start', 'end', 'GeneID', 'Gene', 'Genbank'])
    df3 = df3.drop_duplicates(subset='Gene', keep="last")

    return df3

def append_accession(df):
    
    df3['Accession'] = list(df3[['GeneID', 'Genbank', 'Parent']].to_numpy())
    df3['Accession'] = df3['Accession'].apply(lambda x: [i for i in x if i == i])

    return df3


def extract_parent(attributes_str):
    re_parent = re.compile('Parent=(?P<Parent>.+?);')
    res = re_parent.search(attributes_str)
    return '' if res is None else res.group('Parent')
    
def extract_Dbxref(attributes_str):
    re_Dbxref = re.compile('Dbxref=(?P<Dbxref>.+?);')
    res = re_Dbxref.search(attributes_str)
    return '' if res is None else res.group('Dbxref')
    
def extract_GeneID(Dbxref):
    re_GeneID = re.compile('GeneID:(?P<GeneID>.+?),')
    res = re_GeneID.search(Dbxref)
    return '' if res is None else res.group('GeneID')
    
def extract_Genbank(Dbxref):
    re_Genbank = re.compile('Genbank:(?P<Genbank>.+?),')
    res = re_Genbank.search(Dbxref)
    return '' if res is None else res.group('Genbank')
    
def extract_gene(attributes_str):
    re_Gene = re.compile('Name=(?P<Gene>.+?);')
    res = re_Gene.search(attributes_str)
    return '' if res is None else res.group('Gene')
    
def extract_rgene(attributes_str):
    re_rgene = re.compile('GENEINFO=(?P<gene>.+?);')
    res = re_rgene.search(attributes_str)
    return '' if res is None else res.group('gene')