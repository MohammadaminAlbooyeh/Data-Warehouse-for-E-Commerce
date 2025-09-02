-- Sales Overview View
CREATE OR REPLACE VIEW vw_sales_overview AS
SELECT
    f.order_id,
    d.year,
    d.month,
    d.weekday,
    c.location AS customer_location,
    p.category,
    p.brand,
    f.quantity,
    f.total_amount
FROM FactSales f
JOIN DimDate d ON f.date_id = d.date_id
JOIN DimCustomer c ON f.customer_id = c.customer_id
JOIN DimProduct p ON f.product_id = p.product_id;

-- Customer Insights View
CREATE OR REPLACE VIEW vw_customer_insights AS
SELECT
    c.customer_id,
    c.name,
    c.location,
    c.signup_date,
    SUM(f.total_amount) AS lifetime_value,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM FactSales f
JOIN DimCustomer c ON f.customer_id = c.customer_id
GROUP BY c.customer_id, c.name, c.location, c.signup_date;

-- Product Performance View
CREATE OR REPLACE VIEW vw_product_performance AS
SELECT
    p.product_id,
    p.category,
    p.brand,
    SUM(f.quantity) AS total_units_sold,
    SUM(f.total_amount) AS total_revenue
FROM FactSales f
JOIN DimProduct p ON f.product_id = p.product_id
GROUP BY p.product_id, p.category, p.brand;
