import os
import pandas as pd
from faker import Faker
import random

faker = Faker()

# --- Ensure folders exist ---
os.makedirs("data/bronze", exist_ok=True)

# --- Generate Customers ---
customers = []
for i in range(1, 11):
    customers.append({
        "customer_id": i,
        "name": faker.name(),
        "email": faker.email(),
        "signup_date": faker.date_between(start_date='-2y', end_date='today')
    })

pd.DataFrame(customers).to_csv("data/bronze/customers.csv", index=False)

# --- Generate Products ---
products = []
for i in range(1, 6):
    products.append({
        "product_id": i,
        "name": faker.word(),
        "category": faker.word(),
        "price": round(random.uniform(10, 100), 2)
    })

pd.DataFrame(products).to_csv("data/bronze/products.csv", index=False)

# --- Generate Orders ---
orders = []
for i in range(1, 21):
    orders.append({
        "order_id": i,
        "customer_id": random.randint(1, 10),
        "product_id": random.randint(1, 5),
        "quantity": random.randint(1, 5),
        "total_amount": round(random.uniform(10, 500), 2),
        "order_date": faker.date_between(start_date='-1y', end_date='today')
    })

pd.DataFrame(orders).to_csv("data/bronze/orders.csv", index=False)
