import streamlit as st
import pandas as pd
import time

st.set_page_config(page_title="E-commerce Real-Time Dashboard", layout="wide")
st.title("E-commerce Real-Time Sales Dashboard")

# File path for Gold data
sales_file = "data/gold/sales_summary.csv"
customer_file = "data/gold/customer_ltv.csv"

# Auto-refresh interval (in seconds)
refresh_interval = 5

while True:
    # Load sales data
    sales_data = pd.read_csv(sales_file)
    customer_data = pd.read_csv(customer_file)

    # Display metrics
    total_revenue = sales_data["monthly_revenue"].sum()
    st.metric("Total Revenue", f"${total_revenue:,.2f}")

    top_month = sales_data.sort_values("monthly_revenue", ascending=False).iloc[0]
    st.metric("Top Month", f"{top_month['year_month']} (${top_month['monthly_revenue']:,.2f})")

    # Sales trend chart
    st.line_chart(sales_data.set_index("year_month")["monthly_revenue"])

    # Top 10 customers by lifetime value
    top_customers = customer_data.sort_values("lifetime_value", ascending=False).head(10)
    st.bar_chart(top_customers.set_index("name")["lifetime_value"])

    st.text(f"Last updated: {pd.Timestamp.now()}")

    time.sleep(refresh_interval)
