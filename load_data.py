import pandas as pd
import snowflake.connector

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='YOUR_USER',
    password='YOUR_PASSWORD',
    account='YOUR_ACCOUNT',
    warehouse='YOUR_WAREHOUSE',
    database='YOUR_DB',
    schema='PUBLIC'
)
cursor = conn.cursor()

# Load Customers
df_customers = pd.read_csv("customers.csv")
for _, row in df_customers.iterrows():
    cursor.execute("""
        INSERT INTO DimCustomer(customer_id, name, location, signup_date)
        VALUES (%s, %s, %s, %s)
    """, (row['customer_id'], row['name'], row['location'], row['signup_date']))

# Load Products
df_products = pd.read_csv("products.csv")
for _, row in df_products.iterrows():
    cursor.execute("""
        INSERT INTO DimProduct(product_id, category, brand, unit_price)
        VALUES (%s, %s, %s, %s)
    """, (row['product_id'], row['category'], row['brand'], row['unit_price']))

# Load Orders into FactSales (and DimDate)
df_orders = pd.read_csv("orders.csv")
for _, row in df_orders.iterrows():
    # Insert into DimDate if not exists
    cursor.execute("""
        INSERT INTO DimDate(date_id, day, month, year, weekday)
        SELECT %s, EXTRACT(DAY FROM %s), EXTRACT(MONTH FROM %s),
               EXTRACT(YEAR FROM %s), TO_CHAR(%s, 'Day')
        WHERE NOT EXISTS (SELECT 1 FROM DimDate WHERE date_id = %s)
    """, (row['order_date'], row['order_date'], row['order_date'], row['order_date'], row['order_date'], row['order_date']))
    
    # Insert into FactSales
    cursor.execute("""
        INSERT INTO FactSales(order_id, customer_id, product_id, date_id, quantity, total_amount)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (row['order_id'], row['customer_id'], row['product_id'], row['order_date'], row['quantity'], row['total_amount']))

conn.commit()
cursor.close()
conn.close()
