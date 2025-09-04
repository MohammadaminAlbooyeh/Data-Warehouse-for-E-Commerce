# E-Commerce Data Warehouse Project

## Overview
This project simulates an end-to-end data engineering pipeline for an e-commerce business:
- Generate mock data (customers, products, orders).
- Store data in CSV.
- Load into Snowflake/BigQuery/Redshift.
- Transform into a Star Schema (FactSales + Dimensions).
- Build dashboards in BI tools.
- Automate with Airflow.
- Validate with Great Expectations.

## Steps
1. Run `generate_data.py` → Creates CSVs.
2. Execute `create_tables.sql` in your warehouse.
3. Run `load_data.py` → Loads CSVs into warehouse.
4. Execute `create_views.sql` → Create clean views for BI.
5. Connect Tableau/Power BI to the warehouse.
6. Schedule daily refresh with `dags/ecommerce_etl_dag.py`.
7. Validate with `tests/test_data_quality.py`.

## Deliverables
- Data warehouse with star schema.
- Automated ETL pipeline.
- Business dashboards.
- Data quality checks.
- Documentation.

