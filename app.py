import streamlit as st
import pandas as pd
import numpy as np
import etl_tools.extract as extract
import etl_tools.transform as transform
import etl_tools.load as load

# Expand the web app across the whole screen
st.set_page_config(layout="wide")

st.title('Genome annotator')

st.sidebar.title("vcf_clicer")
st.sidebar.write('Enter the chromosome, start, and end position (ex: 1, 0, 50000)')
input_chromosome = st.sidebar.number_input('Chromosome #', min_value = 1, value = 1)
input_start = st.sidebar.number_input('start position', min_value = 0, value = 0)
input_end = st.sidebar.number_input('end position', min_value = 0, value = 10020)


vcf_data = 'GRCh38_latest_dbSNP_all.vcf.gz'
gff_data = 'GRCh38_latest_genomic.gff.gz'
db_name = 'variant.db'
chr_range = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


def convert_df(df):
   return df.to_csv().encode('utf-8')

if st.button('slice vcf'):
   with st.spinner('Running the slicer.. '):

       df = vcf_utils.vcf_slicer(input_chromosome, input_start, input_end)
    
       st.write(df)

    


if st.button('Run Pipeline'):

    #load.log('ETL Job Started')
    st.write('ETL Job Started')
    load.log('Extract phase started')
    st.write('Extract phase started')
    vcf_raw = extract.extract_vcf(chr_range)
    gff_raw = extract.extract_gff3(gff_data)
    load.log('Extract phase ended')

    load.log('Transform phase started')
    st.write('Transform phase started')
    vcf_pre_processed = transform.vcf_gene_extract(vcf_raw)
    vcf_processed = transform.vcf_transform(vcf_pre_processed)

    gff_processed = (gff_raw.pipe(transform.extract_attributes).
                             pipe(transform.process_attributes).
                             pipe(transform.remove_duplicates).
                             pipe(transform.append_accession))

    load.log('Transform phase ended')
    load.log('Annotating VCF')
    st.write('Annotating VCF')
    vcf_annotated = transform.annotate_vcf(vcf_processed, gff_processed)
    load.log('ETL complete')
    st.write('ETL complete')
    st.write(vcf_annotated)

    convert_df(vcf_annotated)

    

if st.button('Run db'):
    db = load.create_db(db_name)
    st.write(db)