CREATE TABLE FactSales (
    order_id INT,
    customer_id INT,
    product_id INT,
    date_id DATE,
    quantity INT,
    total_amount DECIMAL(10,2),
    PRIMARY KEY(order_id)
);

CREATE TABLE DimCustomer (
    customer_id INT PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    signup_date DATE
);

CREATE TABLE DimProduct (
    product_id INT PRIMARY KEY,
    category VARCHAR,
    brand VARCHAR,
    unit_price DECIMAL(10,2)
);

CREATE TABLE DimDate (
    date_id DATE PRIMARY KEY,
    day INT,
    month INT,
    year INT,
    weekday VARCHAR
);
