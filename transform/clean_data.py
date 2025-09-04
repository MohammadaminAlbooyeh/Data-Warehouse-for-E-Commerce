import pandas as pd

# Load raw data
customers = pd.read_csv("data/bronze/customers.csv")
products = pd.read_csv("data/bronze/products.csv")
orders = pd.read_csv("data/bronze/orders.csv")

# Clean Customers (remove duplicates, nulls)
customers = customers.drop_duplicates(subset=["customer_id"])
customers = customers.dropna(subset=["name", "location"])

# Clean Products (remove invalid prices)
products = products[products["unit_price"] > 0]

# Clean Orders (remove invalid references)
orders = orders[orders["total_amount"] > 0]

# Save to Silver
customers.to_csv("data/silver/customers_clean.csv", index=False)
products.to_csv("data/silver/products_clean.csv", index=False)
orders.to_csv("data/silver/orders_clean.csv", index=False)
