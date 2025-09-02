-- Top 10 Products by Revenue
SELECT p.category, p.brand, SUM(f.total_amount) AS revenue
FROM FactSales f
JOIN DimProduct p ON f.product_id = p.product_id
GROUP BY p.category, p.brand
ORDER BY revenue DESC
LIMIT 10;

-- Monthly Sales Trend
SELECT d.year, d.month, SUM(f.total_amount) AS monthly_sales
FROM FactSales f
JOIN DimDate d ON f.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- Customer Lifetime Value
SELECT c.customer_id, c.name, SUM(f.total_amount) AS lifetime_value
FROM FactSales f
JOIN DimCustomer c ON f.customer_id = c.customer_id
GROUP BY c.customer_id, c.name
ORDER BY lifetime_value DESC
LIMIT 10;
