import great_expectations as ge
import pandas as pd

# Load Silver data
customers = pd.read_csv("data/silver/customers_clean.csv")
products = pd.read_csv("data/silver/products_clean.csv")
orders = pd.read_csv("data/silver/orders_clean.csv")

# Wrap in Great Expectations dataset
customers_ge = ge.from_pandas(customers)
products_ge = ge.from_pandas(products)
orders_ge = ge.from_pandas(orders)

# Customers validations
customers_ge.expect_column_values_to_not_be_null("customer_id")
customers_ge.expect_column_values_to_not_be_null("name")
customers_ge.expect_column_values_to_be_unique("customer_id")

# Products validations
products_ge.expect_column_values_to_be_between("unit_price", min_value=0.01)

# Orders validations
orders_ge.expect_column_values_to_be_between("total_amount", min_value=0.01)
orders_ge.expect_column_values_to_not_be_null("order_id")

# Run & print results
print("✅ Customers Validation:", customers_ge.validate())
print("✅ Products Validation:", products_ge.validate())
print("✅ Orders Validation:", orders_ge.validate())
