import boto3
import json
import requests
from datetime import datetime
from flask_cors import CORS
from flask import Flask, jsonify, request

app = Flask(__name__)
CORS(app)
# AWS clients
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')

# API URLs and key
WEATHER_API_KEY = '97e0978f0194ca2ae82c0bab3532d9aa'  # Replace with your actual API key
AIR_POLLUTION_API_URL = 'http://api.openweathermap.org/data/2.5/air_pollution'
WEATHER_API_URL = 'http://api.openweathermap.org/data/2.5/weather'

BUCKET_NAME = 'climate-data-logs'  # Replace with your actual bucket name
LAMBDA_FUNCTION_NAME = 'processClimateData'  # Replace with your actual Lambda function name

def get_weather(city):
    response = requests.get(f"{WEATHER_API_URL}?q={city}&appid={WEATHER_API_KEY}&units=metric")
    return response.json()

def get_air_quality(lat, lon):
    response = requests.get(f"{AIR_POLLUTION_API_URL}?lat={lat}&lon={lon}&appid={WEATHER_API_KEY}")
    return response.json()

# Modified /api/climate endpoint
@app.route('/api/climate', methods=['GET'])
def get_climate_data():
    city = request.args.get('city', 'London')  # Default city is London if not provided
    
    # Fetch weather data
    weather_data = get_weather(city)
    
    if weather_data.get('cod') != 200:
        return jsonify({"error": "City not found"}), 404
    
    lat = weather_data['coord']['lat']
    lon = weather_data['coord']['lon']
    
    # Fetch air quality data
    air_quality_data = get_air_quality(lat, lon)
    
    # Aggregate data
    climate_data = {
        'city': city,
        'weather': weather_data['weather'][0]['description'],
        'temperature': weather_data['main']['temp'],
        'humidity': weather_data['main']['humidity'],
        'air_quality': air_quality_data['list'][0]['main']['aqi']
    }

    # Log data in S3
    file_name = f"{city}_climate_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=json.dumps(climate_data))
    
    # Trigger AWS Lambda to process the data
    lambda_response = lambda_client.invoke(
        FunctionName=LAMBDA_FUNCTION_NAME,
        InvocationType='Event',  # Asynchronous invocation
        Payload=json.dumps(climate_data)
    )
    
    # Return the aggregated climate data
    return jsonify(climate_data)


if __name__ == '__main__':
    app.run(debug=True)