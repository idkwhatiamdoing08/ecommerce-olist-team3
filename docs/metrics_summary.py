import sqlite3
import pandas as pd

DB_PATH = "data/ecommerce.db"


def calculate_aov(conn):
    """Average Order Value"""
    query = "SELECT AVG(order_total) FROM fact_orders"
    return conn.execute(query).fetchone()[0]


def calculate_retention_m0_m1(conn):
    """Удержание когорты M0 → M1"""
    query = """
    WITH cohort AS (
        SELECT 
            customer_id,
            MIN(DATE(order_purchase_timestamp, 'start of month')) AS cohort_month
        FROM fact_orders
        GROUP BY customer_id
    ),
    monthly_activity AS (
        SELECT 
            customer_id,
            DATE(order_purchase_timestamp, 'start of month') AS activity_month
        FROM fact_orders
        GROUP BY customer_id, activity_month
    )
    SELECT 
        cohort_month,
        COUNT(DISTINCT cohort.customer_id) AS cohort_size,
        COUNT(DISTINCT CASE 
            WHEN activity_month = DATE(cohort_month, '+1 month') 
            THEN cohort.customer_id 
        END) AS retained_m1,
        ROUND(
            COUNT(DISTINCT CASE 
                WHEN activity_month = DATE(cohort_month, '+1 month') 
                THEN cohort.customer_id 
            END) * 100.0 / COUNT(DISTINCT cohort.customer_id), 2
        ) AS retention_rate_m1
    FROM cohort
    LEFT JOIN monthly_activity ON cohort.customer_id = monthly_activity.customer_id
    GROUP BY cohort_month
    ORDER BY cohort_month
    """
    df = pd.read_sql_query(query, conn)
    return df


def calculate_late_delivery_share(conn):
    """Доля опозданий доставки"""
    query = """
    SELECT 
        ROUND(
            SUM(CASE WHEN delivery_time_days > promised_time_days THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
        ) AS late_delivery_percent
    FROM fact_orders
    WHERE delivery_time_days IS NOT NULL AND promised_time_days IS NOT NULL
    """
    return conn.execute(query).fetchone()[0]


def calculate_top_categories(conn, top_n=10):
    """Топ категории по выручке"""
    query = """
    SELECT 
        p.product_category_name,
        SUM(i.price + i.freight_value) AS revenue,
        COUNT(DISTINCT o.order_id) AS orders_count
    FROM fact_orders o
    JOIN fact_order_items i ON o.order_id = i.order_id
    JOIN dim_products p ON i.product_id = p.product_id
    GROUP BY p.product_category_name
    ORDER BY revenue DESC
    LIMIT ?
    """
    return pd.read_sql_query(query, conn, params=(top_n,))


def main():
    try:
        conn = sqlite3.connect(DB_PATH)
    except FileNotFoundError:
        print(f"Database not found at {DB_PATH}. Run ETL first.")
        return

    # Базовые метрики
    orders = pd.read_sql("SELECT * FROM fact_orders", conn)
    gmv = orders['order_total'].sum()
    num_orders = len(orders)
    num_customers = orders['customer_id'].nunique()
    aov = calculate_aov(conn)

    # Новые метрики
    retention_df = calculate_retention_m0_m1(conn)
    late_delivery = calculate_late_delivery_share(conn)
    top_categories = calculate_top_categories(conn, 5)

    print("=" * 60)
    print("E-COMMERCE METRICS SUMMARY")
    print("=" * 60)
    print(f"GMV: {gmv:.2f}")
    print(f"Total Orders: {num_orders}")
    print(f"Unique Customers: {num_customers}")
    print(f"AOV (Average Order Value): {aov:.2f}")
    print(f"Late Delivery Share: {late_delivery}%")
    print("\nTop Categories by Revenue:")
    print(top_categories.to_string(index=False))
    print("\nCohort Retention M0 → M1:")
    print(retention_df.to_string(index=False))
    print("=" * 60)

    conn.close()


if __name__ == "__main__":
    main()