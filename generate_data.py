from faker import Faker
import pandas as pd
import random

fake = Faker()

# Generate Customers
customers = []
for i in range(1000):
    customers.append({
        "customer_id": i+1,
        "name": fake.name(),
        "location": fake.city(),
        "signup_date": fake.date_between(start_date="-2y", end_date="today")
    })

# Generate Products
products = []
categories = ["Electronics", "Fashion", "Home", "Books", "Toys"]
for i in range(200):
    products.append({
        "product_id": i+1,
        "category": random.choice(categories),
        "brand": fake.company(),
        "unit_price": round(random.uniform(5, 500), 2)
    })

# Generate Orders
orders = []
for i in range(5000):
    cust = random.choice(customers)
    prod = random.choice(products)
    qty = random.randint(1, 5)
    orders.append({
        "order_id": i+1,
        "customer_id": cust["customer_id"],
        "product_id": prod["product_id"],
        "order_date": fake.date_between(start_date="-1y", end_date="today"),
        "quantity": qty,
        "total_amount": round(qty * prod["unit_price"], 2)
    })

# Convert to DataFrames
df_customers = pd.DataFrame(customers)
df_products = pd.DataFrame(products)
df_orders = pd.DataFrame(orders)

# Save CSV files
df_customers.to_csv("customers.csv", index=False)
df_products.to_csv("products.csv", index=False)
df_orders.to_csv("orders.csv", index=False)
