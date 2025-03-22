from kafka import KafkaProducer 
import json
import time
import random

producer = KafkaProducer(bootstrap_service='localhost:9092', value_servicer=lambda v: json.dumps(v).encode('utf-8'))

user_ids = [1001, 1002, 1003, 1004, 1005]
locactions = ["Durban", "Cape Town", "London", "Lagos"]
ips = ["192.168.1.1", "203.0.113.42", "8.8.8.8", "123.45.67.89"]

while True:
    transaction = {
        "user_id": random.choice(user_ids),
        "amount": round(random.uniform(5,5000),2),
        "location": random.choice(locations),
        "ip": random.choice(ips),
        "timestamp": time.time()
    }
    producer.send("bank_transactions", value=transaction)
    time.sleep(1)