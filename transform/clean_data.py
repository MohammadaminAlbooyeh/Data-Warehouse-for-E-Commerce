import os
import pandas as pd

# Ensure silver folder exists
os.makedirs("data/silver", exist_ok=True)

# Load raw data from Bronze
customers = pd.read_csv("data/bronze/customers.csv")
products = pd.read_csv("data/bronze/products.csv")
orders = pd.read_csv("data/bronze/orders.csv")

# ----------------------
# Clean Customers
# ----------------------
# Remove duplicates based on customer_id
customers = customers.drop_duplicates(subset=["customer_id"])

# Drop rows with missing essential info
customers = customers.dropna(subset=["name", "email"])

# ----------------------
# Clean Products
# ----------------------
# Remove invalid prices
products = products[products["price"] > 0]

# ----------------------
# Clean Orders
# ----------------------
# Remove orders with invalid total_amount
orders = orders[orders["total_amount"] > 0]

# Optional: remove orders referencing non-existent customers/products
orders = orders[
    orders["customer_id"].isin(customers["customer_id"]) &
    orders["product_id"].isin(products["product_id"])
]

# ----------------------
# Save cleaned data to Silver
# ----------------------
customers.to_csv("data/silver/customers_clean.csv", index=False)
products.to_csv("data/silver/products_clean.csv", index=False)
orders.to_csv("data/silver/orders_clean.csv", index=False)

print("Data cleaned and saved to data/silver/")
