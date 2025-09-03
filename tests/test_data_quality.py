import pandas as pd

# Load CSVs
df_customers = pd.read_csv("customers.csv")
df_orders = pd.read_csv("orders.csv")

# Check: No duplicate customers
assert df_customers["customer_id"].is_unique, "Duplicate customer_id found!"

# Check: All order amounts are positive
assert (df_orders["total_amount"] > 0).all(), "Some orders have invalid total_amount!"

# Check: No null values in critical fields
assert df_orders[["order_id", "customer_id", "product_id"]].notnull().all().all(), "Null values found in key columns"
