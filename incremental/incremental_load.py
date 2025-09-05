import pandas as pd
import snowflake.connector
from datetime import datetime

# --- Config ---
WAREHOUSE = "YOUR_WAREHOUSE"
DATABASE = "YOUR_DB"
SCHEMA = "PUBLIC"

# Read last loaded date
try:
    with open("incremental/last_loaded.txt", "r") as f:
        last_loaded_date = f.read().strip()
        last_loaded_date = datetime.strptime(last_loaded_date, "%Y-%m-%d")
except FileNotFoundError:
    last_loaded_date = datetime(2000, 1, 1)  # Load everything first time

# Load new orders
orders = pd.read_csv("data/silver/orders_clean.csv")
orders["order_date"] = pd.to_datetime(orders["order_date"])
new_orders = orders[orders["order_date"] > last_loaded_date]

if new_orders.empty:
    print("No new data to load.")
    exit()

# Connect to Snowflake
conn = snowflake.connector.connect(
    user="YOUR_USER",
    password="YOUR_PASSWORD",
    account="YOUR_ACCOUNT",
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
)
cursor = conn.cursor()

# Incremental load into FactSales
for _, row in new_orders.iterrows():
    cursor.execute("""
        MERGE INTO FactSales AS target
        USING (SELECT %s AS order_id, %s AS customer_id, %s AS product_id, %s AS date_id, %s AS quantity, %s AS total_amount) AS src
        ON target.order_id = src.order_id
        WHEN MATCHED THEN UPDATE SET
            quantity = src.quantity,
            total_amount = src.total_amount
        WHEN NOT MATCHED THEN
            INSERT (order_id, customer_id, product_id, date_id, quantity, total_amount)
            VALUES (src.order_id, src.customer_id, src.product_id, src.date_id, src.quantity, src.total_amount)
    """, (
        row["order_id"], row["customer_id"], row["product_id"],
        row["order_date"].date(), row["quantity"], row["total_amount"]
    ))

conn.commit()
cursor.close()
conn.close()

# Update last load date
new_last_date = new_orders["order_date"].max().strftime("%Y-%m-%d")
with open("incremental/last_loaded.txt", "w") as f:
    f.write(new_last_date)

print(f"✅ Incremental load complete. Last loaded date updated to {new_last_date}")
