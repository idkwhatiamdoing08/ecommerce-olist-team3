import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "ecommerce.db"


def create_daily_category_mart(conn):
    query = """
    SELECT 
        DATE(o.order_purchase_timestamp) AS order_date,
        p.product_category_name,
        COUNT(DISTINCT o.order_id) AS orders_count,
        COUNT(DISTINCT o.customer_id) AS customers_count,
        SUM(i.price + i.freight_value) AS revenue,
        COUNT(i.order_item_id) AS items_count,
        AVG(i.price + i.freight_value) AS avg_order_value
    FROM fact_orders o
    JOIN fact_order_items i ON o.order_id = i.order_id
    JOIN dim_products p ON i.product_id = p.product_id
    WHERE p.product_category_name IS NOT NULL
    GROUP BY DATE(o.order_purchase_timestamp), p.product_category_name
    ORDER BY order_date DESC, revenue DESC
    """
    df = pd.read_sql_query(query, conn)
    df.to_sql("mart_daily_category", conn, if_exists="replace", index=False)
    print(f"mart_daily_category created: {len(df)} rows")


def create_weekly_city_mart(conn):
    query = """
    SELECT 
        DATE(o.order_purchase_timestamp, 'weekday 0', '-6 days') AS week_start,
        c.customer_city,
        COUNT(DISTINCT o.order_id) AS orders_count,
        COUNT(DISTINCT c.customer_unique_id) AS customers_count,
        SUM(i.price + i.freight_value) AS revenue,
        AVG(i.price + i.freight_value) AS avg_order_value,
        COUNT(i.order_item_id) AS items_count
    FROM fact_orders o
    JOIN dim_customers c ON o.customer_id = c.customer_id
    JOIN fact_order_items i ON o.order_id = i.order_id
    WHERE c.customer_city IS NOT NULL
    GROUP BY week_start, c.customer_city
    ORDER BY week_start DESC, revenue DESC
    """
    df = pd.read_sql_query(query, conn)
    df.to_sql("mart_weekly_city", conn, if_exists="replace", index=False)
    print(f"mart_weekly_city created: {len(df)} rows")


def create_product_performance_mart(conn):
    query = """
    SELECT 
        p.product_id,
        p.product_category_name,
        COUNT(DISTINCT i.order_id) AS orders_count,
        COUNT(DISTINCT o.customer_id) AS customers_count,
        SUM(i.price + i.freight_value) AS total_revenue,
        SUM(i.price) AS product_revenue,
        SUM(i.freight_value) AS freight_revenue,
        AVG(i.price) AS avg_price,
        COUNT(i.order_item_id) AS items_sold
    FROM dim_products p
    JOIN fact_order_items i ON p.product_id = i.product_id
    JOIN fact_orders o ON i.order_id = o.order_id
    GROUP BY p.product_id, p.product_category_name
    ORDER BY total_revenue DESC
    """
    df = pd.read_sql_query(query, conn)
    df.to_sql("mart_product_performance", conn, if_exists="replace", index=False)
    print(f"mart_product_performance created: {len(df)} rows")


def create_delivery_analysis_mart(conn):
    query = """
    SELECT 
        c.customer_city,
        p.product_category_name,
        COUNT(o.order_id) AS orders_count,
        AVG(o.delivery_time_days) AS avg_delivery_days,
        SUM(CASE WHEN o.delivery_time_days > 30 THEN 1 ELSE 0 END) * 100.0 / COUNT(o.order_id) AS late_delivery_percent
    FROM fact_orders o
    JOIN dim_customers c ON o.customer_id = c.customer_id
    JOIN fact_order_items i ON o.order_id = i.order_id
    JOIN dim_products p ON i.product_id = p.product_id
    WHERE o.delivery_time_days IS NOT NULL 
      AND c.customer_city IS NOT NULL 
      AND p.product_category_name IS NOT NULL
    GROUP BY c.customer_city, p.product_category_name
    ORDER BY orders_count DESC
    """
    df = pd.read_sql_query(query, conn)
    df.to_sql("mart_delivery_analysis", conn, if_exists="replace", index=False)
    print(f"mart_delivery_analysis created: {len(df)} rows")

def main():
    try:
        conn = sqlite3.connect(DB_PATH)

        create_daily_category_mart(conn)
        create_weekly_city_mart(conn)
        create_product_performance_mart(conn)
        create_delivery_analysis_mart(conn)

        conn.close()

    except Exception as e:
        print(f"Error creating data marts: {e}")

if __name__ == "__main__":
    main()