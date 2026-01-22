-- Star schema (SQL DDL sample)

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id TEXT PRIMARY KEY,
    customer_unique_id TEXT,
    customer_city TEXT,
    customer_state TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id TEXT PRIMARY KEY,
    product_category_name TEXT,
    product_name_length INT
);

CREATE TABLE IF NOT EXISTS dim_geography (
    city TEXT PRIMARY KEY,
    state TEXT
);

CREATE TABLE IF NOT EXISTS dim_calendar (
    date DATE PRIMARY KEY,
    week INT,
    month INT,
    year INT
);

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id TEXT,
    order_item_id INT,
    customer_id TEXT,
    product_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TIMESTAMP,
    price REAL,
    freight_value REAL,
    delivery_time_days INT,
    PRIMARY KEY(order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS fact_order_items (
    order_id TEXT,
    order_item_id INT,
    product_id TEXT,
    price REAL,
    freight_value REAL,
    PRIMARY KEY(order_id, order_item_id)
);
