import pandas as pd

# Load cleaned data (Silver)
orders = pd.read_csv("data/silver/orders_clean.csv")
customers = pd.read_csv("data/silver/customers_clean.csv")

# Sales Summary (monthly revenue)
orders["order_date"] = pd.to_datetime(orders["order_date"])
orders["year_month"] = orders["order_date"].dt.to_period("M")
sales_summary = orders.groupby("year_month")["total_amount"].sum().reset_index()
sales_summary.rename(columns={"total_amount": "monthly_revenue"}, inplace=True)

# Customer Lifetime Value
customer_ltv = orders.groupby("customer_id")["total_amount"].sum().reset_index()
customer_ltv = customer_ltv.merge(customers[["customer_id", "name"]], on="customer_id")
customer_ltv.rename(columns={"total_amount": "lifetime_value"}, inplace=True)

# Save to Gold
sales_summary.to_csv("data/gold/sales_summary.csv", index=False)
customer_ltv.to_csv("data/gold/customer_ltv.csv", index=False)
