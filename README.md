# vcf-pipeline
#### An ETL pipeline that ingests VCF and GFF3 files to produce an SQL Database of annotated variants.

## Data:
Variant and Gene annotation data was obtained from NCBI’s Human Genome Resources

## Tools used:
I decided to use Streamlit to build a web app for this pipeline so that it can be shared and modifed to print logs, stream data from AWS S3, or build it out into a multipage app.

The core functionality comes from Pandas, while also using Pysam and SQLite3

Code refactoring was done using Sourcery and Autopep8 was used to ensure PEP8 formatting
## Project structure:

## ETL Pipeline:

### Extract:

### Transform:

### Load:

## Reproducing the project:
