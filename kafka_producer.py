import time
import json
from kafka import KafkaProducer
import requests

kafka_bootstrap_servers = 'localhost:9092'
kafka_topic_name = "weather"

# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_weather_detail(openweathermap_api_endpoint):
    try:
        # Make a request to the OpenWeatherMap API
        api_response = requests.get(openweathermap_api_endpoint)
        api_response.raise_for_status()  # Raise an error for bad responses
        json_data = api_response.json()
        
        # Extract necessary details
        city_name = json_data["name"]
        humidity = json_data['main']['humidity']
        temperature = json_data['main']['temp']

        # Create a JSON message
        json_message = {
            "CityName": city_name,
            "Temperature": temperature,
            "Humidity": humidity,
            "CreationTime": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        return json_message
    except Exception as e:
        print(f"Error fetching weather data: {e}")
        return None

def get_appid():
    return "1e4088b035592fca34c1936144afd67b"

# Loop to send messages to Kafka
i = 0
while True:
    cities_name = [
        "Casablanca",
        "Marrakech",
        "Rabat",
        "Fes",
        "Tanger",
        "Agadir"
    ]
    for city in cities_name:
        appid = get_appid()
        openweathermap_api_endpoint = f"https://api.openweathermap.org/data/2.5/weather?appid={appid}&q={city}"
        json_message = get_weather_detail(openweathermap_api_endpoint)

        if json_message:  # Ensure the message is valid before sending
            producer.send(kafka_topic_name, json_message)
            print(f"Published message: {json.dumps(json_message)}")
        else:
            print(f"Skipping message for city: {city} due to errors.")
        
        print("Wait for 5 seconds ...")
        time.sleep(5)


