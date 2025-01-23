import boto3
import requests
import logging
from datetime import datetime
import json

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('weatherdb_table')  # Replace with your table name

API_KEY = 'e5ac149d024d34e91cb21fee00ca1a82'

def fetch_weather(city):
    try:
        url = f'https://api.openweathermap.org/data/2.5/weather?q={city},IN&appid={API_KEY}&units=metric'
        response = requests.get(url)

        if response.status_code == 200:
            weather_data = response.json()
            logger.info(f"Weather data for {city}: {json.dumps(weather_data)}")
            return {
                "City": weather_data.get('name', city),  # Default to city if 'name' is not available
                "Temperature": f"{weather_data['main'].get('temp', 'N/A')}°C",
                "Weather": weather_data['weather'][0].get('description', 'N/A'),
                "Humidity": f"{weather_data['main'].get('humidity', 'N/A')}%",
                "Timestamp": datetime.utcnow().isoformat(),  # ISO format timestamp
                "Date": datetime.utcnow().strftime('%Y-%m-%d')  # Date format as 'YYYY-MM-DD'
            }
        else:
            logger.error(f"Failed to fetch data for {city}: HTTP {response.status_code}")
            return {"Error": f"HTTP {response.status_code}"}
    except Exception as e:
        logger.error(f"Exception for {city}: {str(e)}")
        return {"Error": "Exception occurred"}

def store_weather_data_in_dynamodb(weather_data):
    try:
        # Ensure the primary key 'city' matches the DynamoDB table schema
        item = {
            "city": weather_data["City"],  # Match the partition key
            "timestamp": weather_data["Timestamp"],  # Add sort key if applicable
            "date": weather_data["Date"],  # Add the date key as required by DynamoDB schema
            "temperature": weather_data["Temperature"],
            "weather": weather_data["Weather"],
            "humidity": weather_data["Humidity"]
        }
        table.put_item(Item=item)
        logger.info(f"Stored data in DynamoDB: {item}")
    except Exception as e:
        logger.error(f"Failed to store data in DynamoDB: {str(e)}")


def lambda_handler(event, context):
    cities = [
        'Mumbai', 'Delhi', 'Kolkata', 'Bangalore', 'Chennai', 
        'Hyderabad', 'Pune', 'Ahmedabad', 'Jaipur', 'Surat', 
        'Lucknow', 'Kanpur', 'Nagpur', 'Indore', 'Patna'
    ]
    weather_reports = {}
    
    for city in cities:
        weather_data = fetch_weather(city)
        
        if "Error" not in weather_data:
            # Store data in DynamoDB
            store_weather_data_in_dynamodb(weather_data)
        
        weather_reports[city] = weather_data

    logger.info(f"Weather reports: {json.dumps(weather_reports)}")
    return {
        'statusCode': 200,
        'body': json.dumps(weather_reports)
    }

