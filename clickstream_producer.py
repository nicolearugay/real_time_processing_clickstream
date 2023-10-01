#!/usr/bin/env python3

from confluent_kafka import Producer
import json
import random
import time
import argparse

# Generate random clickstream data
def generate_clickstream_data():
    user_id = random.randint(1, 1000)
    session_id = random.randint(1, 10000)
    timestamp = int(time.time())
    page = random.choice(['homepage', 'product', 'checkout', 'cart'])
    action = random.choice(['view', 'click', 'purchase'])
    
    return {
        'user_id': user_id,
        'session_id': session_id,
        'timestamp': timestamp,
        'page': page,
        'action': action
    }

# Produce data to Kafka
def produce_callback(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Produced record to {msg.topic()}")

# Initialize producer
producer_config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_config)

# Produce data in real-time
while True:
    data = generate_clickstream_data()
    data_json = json.dumps(data)
    
    producer.poll(0)
    producer.produce('clickstream', key=str(data['user_id']), value=data_json, callback=produce_callback)

    time.sleep(2)

# Wait for any outstanding messages to be delivered and delivery reports to be received
producer.flush()
