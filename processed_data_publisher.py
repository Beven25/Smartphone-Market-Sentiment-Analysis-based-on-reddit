from google.cloud import pubsub_v1
import pandas as pd
import json
import re
import time
from google.cloud import storage
import logging
import os

# Create 'logs' directory if it does not exist
logs_dir = 'logs'
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

# Configure logging to write to a file
log_file = os.path.join(logs_dir, 'processed_data_publisher.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()  # Also log to console
    ]
)

# Configuration for both topics
project_id = "dataeng2-407917"
raw_data_subscription_id = "reddit_raw_data-sub"
processed_data_topic_id = "reddit_processed_data"

def clean_text(text):
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r'[^\w\s\.]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def calculate_engagement_score(row):
    score = row['Comment_Score'] + row['Comment_Number_of_Replies'] + row['Comment_Post_Number_of_Comments']
    return score

def preprocess_data(df):
    df['Comment_Body'] = df['Comment_Body'].apply(clean_text)
    df['Post_Title'] = df['Post_Title'].apply(clean_text)
    df['Engagement_Score'] = df.apply(calculate_engagement_score, axis=1)
    return df

def publish_processed_data(project_id, topic_id, processed_data):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    message_bytes = processed_data.to_json(orient='split').encode('utf-8')

    try:
        future = publisher.publish(topic_path, data=message_bytes)
        future.result()  # Wait for publish to complete
        logging.info("Processed data published.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

# Set to keep track of processed message IDs
processed_message_ids = set()
last_message_time = None

def callback(message):
    global last_message_time
    last_message_time = time.time()
    file_path='API data/preprocessed_product.csv'

    # Check for duplicate messages
    if message.message_id in processed_message_ids:
        logging.info(f"Duplicate message {message.message_id} received, skipping.")
        message.ack()
        return

    logging.info("Received raw data message.")

    raw_data = json.loads(message.data.decode("utf-8"))
    df = pd.read_json(raw_data, orient='split')
    logging.info("Data consumed and DataFrame created and saved locally.")

    processed_df = preprocess_data(df)
    processed_df.to_csv(file_path)
    logging.info("Data preprocessing completed.")

    # Authenticate with your service account key JSON file
    client = storage.Client.from_service_account_json('credentials.json')
    bucket_name = 'data_eng2_bucket'
    destination_blob_name = 'preprocessed_product_data.csv'
    
    # Upload the CSV file to GCS
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    try:
        blob.upload_from_filename(file_path)
        logging.info("File uploaded to GCS.")
    except Exception as e:
        logging.error(f"Error uploading file to GCS: {e}")

    publish_processed_data(project_id, processed_data_topic_id, processed_df)
    message.ack()
    logging.info("Message acknowledged and processing completed.")

    # Add message ID to the set of processed messages
    processed_message_ids.add(message.message_id)

def main():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, raw_data_subscription_id)

    with subscriber:
        future = subscriber.subscribe(subscription_path, callback=callback)
        logging.info(f"Listening for messages on {subscription_path}...")

        while True:
            time.sleep(5)  # Wait for 5 seconds between checks
            if last_message_time and (time.time() - last_message_time > 30):  # 30 seconds timeout
                logging.info("No new messages for 30 seconds, exiting.")
                break

        future.cancel()  # Stop the subscriber

if __name__ == "__main__":
    main()
