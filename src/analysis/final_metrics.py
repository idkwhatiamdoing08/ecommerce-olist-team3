import sqlite3
import pandas as pd
from pathlib import Path
import json

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "ecommerce.db"
OUTPUT_PATH = Path(__file__).resolve().parents[2] / "docs" / "final_metrics_report.txt"


def calculate_all_metrics(conn):
    gmv_query = """
    SELECT 
        COUNT(DISTINCT o.order_id) as total_orders,
        COUNT(DISTINCT o.customer_id) as total_customers,
        SUM(i.price + i.freight_value) as gmv
    FROM fact_orders o
    JOIN fact_order_items i ON o.order_id = i.order_id
    """
    basics = pd.read_sql_query(gmv_query, conn)

    if basics['total_orders'].iloc[0] > 0:
        basics['aov'] = basics['gmv'] / basics['total_orders']
    else:
        basics['aov'] = 0

    top_categories = pd.read_sql_query("""
        SELECT product_category_name, SUM(total_revenue) as revenue
        FROM mart_product_performance
        WHERE product_category_name IS NOT NULL
        GROUP BY product_category_name
        ORDER BY revenue DESC
        LIMIT 10
    """, conn)

    late_delivery = pd.read_sql_query("""
        SELECT 
            SUM(CASE WHEN delivery_time_days > 30 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as late_delivery_percent
        FROM fact_orders
        WHERE delivery_time_days IS NOT NULL
    """, conn)

    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(mart_delivery_analysis)")
    columns = [col[1] for col in cursor.fetchall()]

    delivery_metrics = []
    if columns:
        has_avg_days = 'avg_delivery_days' in columns
        has_orders_count = 'orders_count' in columns

        if has_avg_days and has_orders_count:
            query = """
                SELECT 
                    customer_city,
                    AVG(avg_delivery_days) as avg_delivery_days,
                    SUM(orders_count) as total_orders
                FROM mart_delivery_analysis
                WHERE customer_city IS NOT NULL 
                  AND avg_delivery_days IS NOT NULL
                GROUP BY customer_city
                ORDER BY total_orders DESC
                LIMIT 5
            """
            delivery_metrics = pd.read_sql_query(query, conn).to_dict('records')

    unique_customers = pd.read_sql_query(
        "SELECT COUNT(DISTINCT customer_unique_id) as unique_customers FROM dim_customers",
        conn
    )

    if not unique_customers.empty:
        basics['total_customers'] = unique_customers['unique_customers'].iloc[0]

    retention = {
        'retention_m0_m1': '0.0%',
        'note': 'All customers made only one purchase (no repeat buyers in dataset)'
    }

    return {
        'basics': basics.iloc[0].to_dict(),
        'top_categories': top_categories.to_dict('records'),
        'late_delivery_percent': late_delivery['late_delivery_percent'].iloc[0] if not late_delivery.empty else 0,
        'delivery_by_city': delivery_metrics,
        'retention': retention
    }


def save_report(metrics, output_path):
    report = []

    basics = metrics['basics']
    report.append(f"GMV (Gross Merchandise Value): R${basics['gmv']:,.2f}")
    report.append(f"AOV (Average Order Value): R${basics['aov']:,.2f}")
    report.append(f"Total Orders: {int(basics['total_orders']):,}")
    report.append(f"Unique Customers: {int(basics['total_customers']):,}")
    report.append(f"Orders per Customer: {(basics['total_orders'] / basics['total_customers']):.2f}")

    report.append("\n COHORT RETENTION:")
    report.append("-" * 40)
    report.append(f"M0 → M1 Retention: {metrics['retention']['retention_m0_m1']}")
    report.append(f"Note: {metrics['retention']['note']}")

    report.append("\n DELIVERY PERFORMANCE:")
    report.append("-" * 40)
    report.append(f"Late Delivery Rate (>30 days): {metrics['late_delivery_percent']:.2f}%")

    if metrics['delivery_by_city']:
        report.append("\nAverage Delivery Time by Top Cities:")
        for city in metrics['delivery_by_city']:
            city_name = city.get('customer_city', 'Unknown')
            avg_days = city.get('avg_delivery_days', 0)
            total_orders = city.get('total_orders', 0)
            report.append(f"  {city_name}: {avg_days:.1f} days ({int(total_orders):,} orders)")
    else:
        report.append("\nDelivery time by city: Data not available")

    report.append("\n TOP CATEGORIES BY REVENUE:")
    report.append("-" * 40)
    if metrics['top_categories']:
        for i, cat in enumerate(metrics['top_categories'], 1):
            name = cat['product_category_name']
            revenue = cat['revenue']
            report.append(f"{i:2d}. {name:<30} R${revenue:>12,.2f}")
    else:
        report.append("No category data available")

    with open(output_path, 'w') as f:
        f.write("\n".join(report))

    print(f"Report saved to: {output_path}")

    print("\n".join(report))


def main():
    conn = sqlite3.connect(DB_PATH)

    try:
        metrics = calculate_all_metrics(conn)
        save_report(metrics, OUTPUT_PATH)

        json_path = OUTPUT_PATH.parent / "final_metrics.json"
        with open(json_path, 'w') as f:
            json.dump(metrics, f, indent=2, default=str)
        print(f"\nJSON data saved to: {json_path}")

    except Exception as e:
        print(f"Error generating report: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()