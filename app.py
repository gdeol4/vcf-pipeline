import streamlit as st
import etl_tools.extract as extract
import etl_tools.transform as transform
import etl_tools.load as load
import etl_tools.sql_queries as sql_query



# Expand the web app across the whole screen
#st.set_page_config(layout="wide")

st.title('Genome annotator')

st.sidebar.title("vcf_clicer")
st.sidebar.write('Enter the chromosome, start, and end position (ex: 1, 0, 10020)')
input_chromosome = st.sidebar.selectbox('Choose a chromosome:', ('1', '2', '3', '4', 
                                                                 '5', '6', '7', '8', 
                                                                 '9', '10', '11', '12', 
                                                                 '13', '14', '15', '16', 
                                                                 '17', '18', '19', '20', 
                                                                 '21', '22', 'X', 'Y'))
input_start = st.sidebar.number_input('start position', min_value = 0, max_value = 49999, value = 0)
input_end = st.sidebar.number_input('end position', min_value = 1, max_value = 50000, value = 10020)

vcf_data = 'GRCh38_latest_dbSNP_all.vcf.gz'
gff_data = 'GRCh38_latest_genomic.gff.gz'
db_name = 'variant.db'
chr_range = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10,]


if st.button('slice vcf'):

   if str.isdigit(input_chromosome) == True:
    input_chromosome = int(input_chromosome)
   else:
    pass
        
   df = extract.vcf_slicer(input_chromosome, input_start, input_end)
   df = extract.slice_format(df)
   st.write(df)

run_pipeline = st.button('Run Pipeline')

if run_pipeline:

    #load.log('ETL Job Started')
    st.markdown('#### ETL Job Started 🚦')

    st.markdown('#### Extract phase started ✅')
    vcf_raw = extract.extract_vcf(chr_range)
    gff_raw = extract.extract_gff3(gff_data)

    st.markdown('#### Transform phase started ✅')
    vcf_pre_processed = transform.vcf_gene_extract(vcf_raw)
    vcf_processed = transform.vcf_transform(vcf_pre_processed)

    gff_processed = (gff_raw.pipe(transform.extract_attributes).
                             pipe(transform.process_attributes).
                             pipe(transform.remove_duplicates).
                             pipe(transform.append_accession))

    st.markdown('#### Annotating VCF ✅')
    vcf_annotated = transform.annotate_vcf(vcf_processed, gff_processed)
    st.markdown('#### Load phase started ✅')
    load.create_db(db_name, vcf_annotated)
    st.markdown('#### ETL complete 🏁')
    st.markdown('##### VCF head')
    st.write(vcf_annotated.head())
    st.markdown('##### Uniprot sequences')
    gene_list = set(vcf_annotated.Gene.tolist())
    uniprot_seq = load.get_uniprot_sequences(gene_list)
    st.write(load.process_uniprot(uniprot_seq))
  #  st.write(uniprot_final)


    
with st.expander("Question 1: Chromosome with the most variants."):
     st.write("""
         The chart above shows some numbers I picked for you.
         I rolled actual dice for these, so they're *guaranteed* to
         be random.
     """)
     st.write(sql_query.query_1(db_name))

with st.expander("Question 2: First variant in each chromosome."):
     st.write("""
         The chart above shows some numbers I picked for you.
         I rolled actual dice for these, so they're *guaranteed* to
         be random.
     """)
     st.write(sql_query.query_2(db_name))

with st.expander("Question 3: Most common variant type per chromosome"):
     st.write("""
         The chart above shows some numbers I picked for you.
         I rolled actual dice for these, so they're *guaranteed* to
         be random.
     """)


with st.expander("Question 4: Most common variant type overall"):
     st.write("""
         The chart above shows some numbers I picked for you.
         I rolled actual dice for these, so they're *guaranteed* to
         be random.
     """)
     st.write(sql_query.query_4(db_name))
     

with st.expander("Question 5: Genes with the most variants"):
     st.write("""
         The chart above shows some numbers I picked for you.
         I rolled actual dice for these, so they're *guaranteed* to
         be random.
     """)
     st.write(sql_query.query_5(db_name))
