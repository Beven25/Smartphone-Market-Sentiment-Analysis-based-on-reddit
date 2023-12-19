import logging
from google.cloud import bigquery

# Set up logging configuration
logging.basicConfig(filename='logs/bigquery_load.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

client = bigquery.Client.from_service_account_json('credentials.json')  
dataset_id = 'dataeng2-407917.dataset_dataeng2'  
table_id = 'table_all_products'  

# Configure the job to load data from GCS and auto-detect schema
job_config = bigquery.LoadJobConfig(
    autodetect=True,  
    skip_leading_rows=1,  
    source_format=bigquery.SourceFormat.CSV  
)

uri = 'gs://data_eng2_bucket/final_product.csv'  

try:
    # Create a job to load data from GCS to BigQuery
    load_job = client.load_table_from_uri(
        uri, f"{dataset_id}.{table_id}", job_config=job_config
    )

    load_job.result()  

    logging.info(f"Loaded {load_job.output_rows} rows into {dataset_id}.{table_id}")
    print(f"Loaded {load_job.output_rows} rows into {dataset_id}.{table_id}")

except Exception as e:
    logging.error(f"Error loading data to BigQuery: {str(e)}")
    print(f"Error loading data to BigQuery: {str(e)}")
