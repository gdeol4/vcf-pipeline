import streamlit as st
import etl_tools.extract as extract
import etl_tools.transform as transform
import etl_tools.load as load
import etl_tools.sql_queries as sql_query



# Expand the web app across the whole screen
#st.set_page_config(layout="wide")

st.title('Variant annotator')
#https://www.ncbi.nlm.nih.gov/grc/humanv
#http://bigg.ucsd.edu/genomes/ncbi_assembly:GCF_000001405.33

st.header('Introduction 👋')
with st.expander('About me'):
    st.subheader('Education 👨‍🎓️')
    st.write('University of Western Ontario - Genetics')
    st.write('Universty of Guelph - Bioinformatics')
    st.subheader('Work experience 💼')
    st.write('Cyclica - Analyst')
    st.write('Loblaw & Walmart - Data Analyst')
    st.write('Flexport - Data Engineer')
    st.write('Bell - Data engineer')

st.header('Structuring the project')

with st.expander('Project structure'):
    st.subheader('Tools 🔨')
    st.write('''I decided to use Streamlit to build a web app for this pipeline
                so that it can be shared and modifed to print logs, stream data
                from AWS S3, or build it out into a multipage app.''')
    st.write('''The core functionality comes from Pandas, while also using Pysam and SQLite3''')
    st.write('''Code refactoring was done using Sourcery and Autopep8 was used to ensure PEP8 formatting''')

    st.subheader('Data 📊')
    st.write('''Variant and Gene annotation data was obtained from NCBI’s Human Genome Resources''')

with st.expander('ETL Process'):
    st.image('etl.png', caption='ETL process flow')
    st.subheader('ETL steps ⚙️')
    st.write('''The pipeline is built using three modules that are aptly named: extract, transform, load''')
    st.markdown('#### Extract:')
    st.write('''The extraction phase calls to main functions to parse the data
                and to create pandas dataframes for both files''')
    st.markdown('###### VCF:')
    st.code('vcf_raw = extract.extract_vcf(chr_range)', language = 'python')
    st.markdown('###### GFF:')
    st.code('gff_raw = extract.extract_gff3(gff_data)', language = 'python')

    st.markdown('#### Transform:')
    st.write('''The transformation phase formats the VCF first and then goes on
                create pandas dataframes for both files. The next part of the pipeline
                takes the gff dataframe as input into the pandas pipe, which is a group of
                functions that take the output of the preceeding function as their
                input.''')
    st.write('''The annotation step merges  the two processed dataframes on the Gene column''')

    st.markdown('###### VCF:')
    st.code('vcf_pre_processed = transform.vcf_gene_extract(vcf_raw)', language = 'python')
    st.markdown('###### GFF:')
    st.code('vcf_processed = transform.vcf_transform(vcf_pre_processed)', language = 'python')
    st.code('''gff_processed = (gff_raw.pipe(transform.extract_attributes).
                         pipe(transform.process_attributes).
                         pipe(transform.remove_duplicates).
                         pipe(transform.append_accession))''')
    st.markdown('###### Annotation:')
    st.code('vcf_annotated = transform.annotate_vcf(vcf_processed, gff_processed)', language = 'python')

    st.markdown('##### Load:')
    st.write('''The load phase simply creates an SQLite3 database and the 'to_sql' function
                handles the upload to the table, taking it from dataframe to table''')
    st.code('load.create_db(db_name, vcf_annotated)', language = 'python')
    st.markdown('###### Uniprot Sequence:')
    st.write('''Lastly, a list of genes is parameterized and sent as a uniprot query to return protein sequences.''')
    st.code('gene_list = set(vcf_annotated.Gene.tolist())', language = 'python')
    st.code('uniprot_seq = load.get_uniprot_sequences(gene_list)', language = 'python')

st.header('Subsetting a VCF')
st.write('''The function to subset a VCF uses Pysam and Tabix to first index
            the .gz file before creating a Tabix iterator which stores records as a dataframe: ''')
st.write('''After the chromosome, start, and end positions have been selected, the button below will run the function:''')



st.sidebar.title("VCF slicer")
st.sidebar.write('Enter the chromosome, start, and end position (ex: 1, 0, 10020)')
input_chromosome = st.sidebar.selectbox('Choose a chromosome:', ('1', '2', '3', '4', 
                                                                 '5', '6', '7', '8', 
                                                                 '9', '10', '11', '12', 
                                                                 '13', '14', '15', '16', 
                                                                 '17', '18', '19', '20', 
                                                                 '21', '22', 'X', 'Y'))
input_start = st.sidebar.number_input('start position', min_value = 0, max_value = 3050000, value = 0)
input_end = st.sidebar.number_input('end position', min_value = 1, max_value = 3055000, value = 10020)

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

st.header("ETL pipeline")

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

st.header("SQL queries")

with st.expander("Question 1: Chromosome with the most variants."):
     st.write(sql_query.query_1(db_name))

with st.expander("Question 2: First variant in each chromosome."):
     st.write(sql_query.query_2(db_name))

with st.expander("Question 3: Most common variant type per chromosome"):
     st.write(sql_query.query_3(db_name))

with st.expander("Question 4: Most common variant type overall"):
     st.write(sql_query.query_4(db_name))
     
with st.expander("Question 5: Genes with the most variants"):
     st.write(sql_query.query_5(db_name))
