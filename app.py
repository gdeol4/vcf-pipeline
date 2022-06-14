import streamlit as st
import pandas as pd
import numpy as np
import pysam
import vcf_utils

# Expand the web app across the whole screen
st.set_page_config(layout="wide")

st.title('Genome annotator')

st.sidebar.title("vcf_clicer")

option = st.selectbox(
     'How would you like to be contacted?',
     ('Email', 'Home phone', 'Mobile phone'))

st.write('You selected:', option)

vcf_data = 'GRCh38_latest_dbSNP_all.vcf.gz'

chromosome = st.number_input('Chromosome #')
start = st.number_input('start position')
end = st.number_input('end position')

if st.button('slice vcf'):
    with st.spinner('Running the slicer.. '):

        df = vcf_utils.vcf_slicer(chromosome, start, end)
    
        st.write(df)


#tbx = pysam.TabixFile(vcf_data)

    
col_names = ['seqid', 
             'source', 
             'type', 
             'start', 
             'end', 
             'score', 
             'strand', 
             'phase', 
             'attributes']

st.sidebar.title("Pipeline")

# Data cleaning options
st.sidebar.subheader("Data transforming")
scale = st.sidebar.checkbox("Scale", False, "scale")
encode = st.sidebar.checkbox("Encode", False, "encode")
impute = st.sidebar.checkbox("Impute", False, "impute")


