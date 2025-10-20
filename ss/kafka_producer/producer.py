import requests
import psycopg2
from kafka import KafkaProducer
import json
import time

# ---- Kafka Producer ----
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ---- PostgreSQL Connection ----
conn = psycopg2.connect(
    host="postgres",  # Docker service name
    database="weather_db",
    user="airflow",
    password="airflow"
)
cursor = conn.cursor()

# ---- Function to fetch weather ----
def get_weather(city="London"):
    api_key = "YOUR_OPENWEATHERMAP_API_KEY"
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    response = requests.get(url).json()
    data = {
        "city": city,
        "temperature": response["main"]["temp"],
        "humidity": response["main"]["humidity"],
        "condition": response["weather"][0]["description"]
    }
    return data

# ---- Main loop ----
cities = ["London", "New York", "Delhi"]

while True:
    for city in cities:
        weather = get_weather(city)
        
        # Send to Kafka
        producer.send("weather_topic", weather)
        
        # Insert into Postgres
        cursor.execute(
            """
            INSERT INTO weather_data (city, temperature, humidity, condition)
            VALUES (%s, %s, %s, %s)
            """,
            (weather["city"], weather["temperature"], weather["humidity"], weather["condition"])
        )
        conn.commit()
        
        print(f"Data sent for {city}: {weather}")
    time.sleep(60)  # wait 1 min before next fetch
