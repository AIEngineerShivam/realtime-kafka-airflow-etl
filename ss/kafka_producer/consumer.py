# consumer.py
from kafka import KafkaConsumer
import json

# Connect to Kafka broker
consumer = KafkaConsumer(
    'weather_topic',                  # Topic name
    bootstrap_servers='kafka:9092',   # Must match producer
    auto_offset_reset='earliest',     # Read messages from the beginning
    group_id='weather_consumer_group',# Consumer group
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))  # Deserialize JSON to dict
)

print("Listening to Kafka topic 'weather_topic'...")

# Consume messages
for message in consumer:
    print(f"Received: {message.value}")
