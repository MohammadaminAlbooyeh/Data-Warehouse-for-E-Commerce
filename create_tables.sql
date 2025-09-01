-- Dimension Tables
CREATE TABLE DimCustomer (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100),
    location VARCHAR(100),
    signup_date DATE
);

CREATE TABLE DimProduct (
    product_id INT PRIMARY KEY,
    category VARCHAR(50),
    brand VARCHAR(100),
    unit_price DECIMAL(10,2)
);

CREATE TABLE DimDate (
    date_id DATE PRIMARY KEY,
    day INT,
    month INT,
    year INT,
    weekday VARCHAR(10)
);

-- Fact Table
CREATE TABLE FactSales (
    order_id INT PRIMARY KEY,
    customer_id INT REFERENCES DimCustomer(customer_id),
    product_id INT REFERENCES DimProduct(product_id),
    date_id DATE REFERENCES DimDate(date_id),
    quantity INT,
    total_amount DECIMAL(10,2)
);
