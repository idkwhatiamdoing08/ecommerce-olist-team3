import pandas as pd
import sqlite3
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
DB_PATH = DATA_DIR / "ecommerce.db"

def load_csv(name):
    path = DATA_DIR / name
    if not path.exists():
        raise FileNotFoundError(f"{path} not found")
    return pd.read_csv(path)

def transform_orders(df_orders):
    # Normalize timestamps
    df_orders['order_purchase_timestamp'] = pd.to_datetime(df_orders['order_purchase_timestamp'], errors='coerce')
    # Lower statuses
    df_orders['order_status'] = df_orders['order_status'].str.lower().fillna('unknown')
    # Remove exact duplicate order_items rows
    df_orders = df_orders.drop_duplicates(subset=['order_id','order_item_id'])
    # Remove price anomalies
    if 'price' in df_orders.columns:
        df_orders = df_orders[df_orders['price'] > 0]
    # Compute delivery_time_days if not present
    if 'delivery_time_days' not in df_orders.columns and 'order_delivered_customer_date' in df_orders.columns:
        df_orders['order_delivered_customer_date'] = pd.to_datetime(df_orders['order_delivered_customer_date'], errors='coerce')
        df_orders['delivery_time_days'] = (df_orders['order_delivered_customer_date'] - df_orders['order_purchase_timestamp']).dt.days.fillna(0).astype(int)
    return df_orders

def upsert_sqlite(df_orders, df_customers, df_products, df_items):
    conn = sqlite3.connect(DB_PATH)
    df_orders.to_sql("fact_orders", conn, if_exists="replace", index=False)
    df_items.to_sql("fact_order_items", conn, if_exists="replace", index=False)
    df_customers.to_sql("dim_customers", conn, if_exists="replace", index=False)
    df_products.to_sql("dim_product", conn, if_exists="replace", index=False)
    conn.close()

def main():
    print("ETL: starting")
    orders = load_csv("olist_orders.csv")
    customers = load_csv("olist_customers.csv")
    products = load_csv("olist_products.csv")
    items = load_csv("olist_order_items.csv")

    print("ETL: raw shapes:", orders.shape, customers.shape, products.shape, items.shape)
    orders = transform_orders(orders)
    print("ETL: after transform orders shape:", orders.shape)

    # Ensure data dir exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    upsert_sqlite(orders, customers, products, items)
    print("ETL: finished. SQLite DB written at", DB_PATH)

if __name__ == "__main__":
    main()
