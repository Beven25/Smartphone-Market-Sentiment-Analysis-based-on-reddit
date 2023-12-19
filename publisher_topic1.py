import pandas as pd
import json
from google.cloud import pubsub_v1
from google.cloud import storage
import logging
import os

# Create 'logs' directory if it does not exist
logs_dir = 'logs'
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

# Configure logging to write to a file
log_file = os.path.join(logs_dir, 'publisher_topic1.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()  
    ]
)

df = pd.read_csv('API data/raw_data.csv', index_col=[0])

def validate_data(row, df_dtypes):
    try:
        for field in df_dtypes.keys():
            # Check for integer fields
            if df_dtypes[field] == 'int64':
                # Allow negative values for Comment_Score and Post_Score
                if field not in ['Comment_Score', 'Post_Score']:
                    if not isinstance(row[field], int) or row[field] < 0:
                        raise ValueError(f"Invalid value in {field}: {row[field]}")
            
            # Check for string fields
            elif df_dtypes[field] == 'object':
                if not isinstance(row[field], str):
                    raise ValueError(f"Invalid value in {field}: {row[field]}")
            
    except ValueError as e:
        logging.error(f"Data validation error in row {row.name}: {e}")
        raise

df_dtypes = df.dtypes

for index, row in df.iterrows():
    validate_data(row, df_dtypes)

# Configuration
project_id = "dataeng2-407917"
topic_id = "reddit_raw_data"
file_path = "API data/raw_data.csv"

def read_csv(file_path):
    return pd.read_csv(file_path)

def publish_message(project_id, topic_id, message):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    
    # Convert DataFrame row to JSON
    message_bytes = json.dumps(message).encode('utf-8')

    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        publish_future.result()  
        logging.info("Message published.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

    df = read_csv(file_path)
    client = storage.Client.from_service_account_json('credentials.json')
    bucket_name = 'data_eng2_bucket'
    destination_blob_name = 'raw_product_data.csv'
    
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    try:
        blob.upload_from_filename(file_path)
        logging.info("File uploaded to GCS.")
    except Exception as e:
        logging.error(f"Error uploading file to GCS: {e}")

def main():
    df = read_csv(file_path)
    df_json = df.to_json(orient='split')
    publish_message(project_id, topic_id, df_json)

if __name__ == "__main__":
    main()
