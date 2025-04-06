import logging
from google.cloud import bigquery

logging.basicConfig(filename='logs/bigquery_query.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
client = bigquery.Client.from_service_account_json('credentials.json')  

dataset_id = 'dataeng2-407917.dataset_dataeng2' 
table_id = 'table_all_products' 


table_ref = client.dataset(dataset_id).table(table_id)
#Query to create to seperate tables for Iphone15 and Samsung S23 in BigQuery
query = """
CREATE OR REPLACE TABLE dataeng2-407917.dataset_dataeng2.table_iphone AS
SELECT *
FROM dataeng2-407917.dataset_dataeng2.table_all_products
WHERE Comment_Subreddit = 'iphone15';

CREATE OR REPLACE TABLE dataeng2-407917.dataset_dataeng2.table_samsung AS
SELECT *
FROM dataeng2-407917.dataset_dataeng2.table_all_products
WHERE Comment_Subreddit = 'GalaxyS23';

"""

try:
    # Run the query
    query_job = client.query(query)

    # Wait for the query to complete and fetch the results
    results = query_job.result()
    logging.info("Successfully ran the query")
    print("Successfully ran the query")

except Exception as e:
    logging.error("Error occurred: %s", str(e))
    print("An error occurred:", e)
