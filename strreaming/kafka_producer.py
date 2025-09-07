from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Mock data
customers = [1, 2, 3, 4, 5]
products = [101, 102, 103, 104]

while True:
    order = {
        "order_id": random.randint(1000, 9999),
        "customer_id": random.choice(customers),
        "product_id": random.choice(products),
        "quantity": random.randint(1, 5),
        "total_amount": round(random.uniform(10.0, 200.0), 2),
        "order_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    producer.send("orders_topic", order)
    print(f"Produced: {order}")
    time.sleep(2)  # produce an order every 2s
