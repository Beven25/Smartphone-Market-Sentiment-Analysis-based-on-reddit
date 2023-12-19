
import logging
from google.cloud import pubsub_v1
import pandas as pd
import json
from google.cloud import storage
import time
import sys

# Set up logging configuration
logging.basicConfig(filename='logs/pull_topic_2.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
project_id = "dataeng2-407917"
subscription_id = "reddit_processed_data-sub"
processed_message_ids = set()

def callback(message):
    global last_message_time
    last_message_time = time.time()
    if message.message_id in processed_message_ids:
        logging.info(f"Duplicate message {message.message_id} received, skipping.")
        message.ack()
        return  
    
    try:
        # Decode the message data from bytes to a string, then load it into a JSON object
        json_data = json.loads(message.data.decode("utf-8"))
        # Convert the JSON object back to a JSON string
        json_str = json.dumps(json_data)
        # Load the JSON string into a DataFrame
        df = pd.read_json(json_str, orient='split')
        logging.info("DataFrame reconstructed. Dimensions: %s", str(df.shape))
        

        #implementing sentiment analysis
        #df = df[df['Comment_Subreddit'] == 'iphone15']
        #classifier = pipeline("zero-shot-classification")

        # Sample candidate labels
        # candidate_labels = ["happy", "excited", "satisfied", "frustrated", "angry", "disappointed", "neutral", "indifferent", "ambivalent"]

        # # Function to classify sentiments
        # def classify_sentiment(sequence):
        #     result = classifier(sequence, candidate_labels)
        #     return result['labels'][0]  # Extracting the top predicted label
        # df['Sentiment'] = df['Comment_Body'].apply(classify_sentiment)

        
        # Read CSV data
        df = pd.read_csv('API data/final_product.csv')
        
        client = storage.Client.from_service_account_json('credentials.json')  
        
        bucket_name = 'data_eng2_bucket'  
        destination_blob_name = 'final_product.csv'  
        
        # Upload the CSV file to GCS
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename('API data/final_product.csv')
        
        logging.info("DataFrame pushed to GCS successfully")
    
    except Exception as e:
        logging.error("Error processing message: %s", str(e))

def main():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    logging.info(f"Listening for messages on {subscription_path}...")

    try:
        while True:
            # Check for messages indefinitely until subscription is exhausted
            streaming_pull_future.result(timeout=60)  
    except Exception as e:
        streaming_pull_future.cancel()

    logging.shutdown()  # Flush logs
    sys.exit(0)  

if __name__ == "__main__":
    main()
