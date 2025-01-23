import boto3
import logging
import json
from datetime import datetime
import re

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client
s3 = boto3.client('s3')
S3_BUCKET_NAME = 'weather-streambucket'  # Replace with your S3 bucket name

def process_dynamodb_stream(event):
    for record in event['Records']:
        event_name = record['eventName']  # INSERT, MODIFY, REMOVE
        timestamp = datetime.utcnow().isoformat()  # Add timestamp for versioning
        record_data = {
            "EventName": event_name,
            "Timestamp": timestamp
        }

        city_name = ""  # Variable to store city name

        if event_name == 'INSERT':
            if 'NewImage' in record['dynamodb']:
                record_data['NewItem'] = record['dynamodb']['NewImage']
                # Extract city name from NewItem
                city_name = record['dynamodb']['NewImage'].get('city', {}).get('S', 'Unknown City')
                logger.info(f"New item inserted for {city_name}: {json.dumps(record_data)}")
        elif event_name == 'MODIFY':
            if 'NewImage' in record['dynamodb']:
                record_data['NewItem'] = record['dynamodb']['NewImage']
            if 'OldImage' in record['dynamodb']:
                record_data['OldItem'] = record['dynamodb']['OldImage']
            # Extract city name from NewItem
            city_name = record['dynamodb']['NewImage'].get('city', {}).get('S', 'Unknown City')
            logger.info(f"Item modified for {city_name}: {json.dumps(record_data)}")
        elif event_name == 'REMOVE':
            if 'OldImage' in record['dynamodb']:
                record_data['OldItem'] = record['dynamodb']['OldImage']
            # Extract city name from OldItem
            city_name = record['dynamodb']['OldImage'].get('city', {}).get('S', 'Unknown City')
            logger.info(f"Item removed for {city_name}: {json.dumps(record_data)}")

        # Sanitize city name for S3 object key
        city_name = re.sub(r'[^a-zA-Z0-9_-]', '_', city_name)

        # Store the record in S3
        store_data_in_s3(record_data, event_name, timestamp, city_name)

def store_data_in_s3(record_data, event_name, timestamp, city_name):
    try:
        # Define the S3 key (include the city name in the S3 path)
        key = f"weather_data/{event_name}/{city_name}/{timestamp}.json"
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=key,
            Body=json.dumps(record_data),
            ContentType='application/json'
        )
        logger.info(f"Stored record in S3 for {city_name}: {key}")
    except Exception as e:
        logger.error(f"Failed to store record in S3 for {city_name}: {str(e)}")

def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")
    process_dynamodb_stream(event)
    return {
        'statusCode': 200,
        'body': json.dumps('Stream records processed and stored in S3')
    }

