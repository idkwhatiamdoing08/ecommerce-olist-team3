import pandas as pd
import sqlite3
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
DB_PATH = DATA_DIR / "ecommerce.db"

def load_csv(name):
    path = DATA_DIR / name
    if not path.exists():
        raise FileNotFoundError(f"{path} not found")
    return pd.read_csv(path, low_memory=False)

def deduplicate_customers(df_customers):
    print("Deduplicating customers...")
    report = []
    initial_count = len(df_customers)

    mask_duplicate_id = df_customers.duplicated(subset=['customer_unique_id'], keep=False)
    duplicate_ids = df_customers[mask_duplicate_id]

    if not duplicate_ids.empty:
        dup_count = len(duplicate_ids)
        report.append(f"Exact duplicates by customer_unique_id: {dup_count} records")
        dup_groups = duplicate_ids.groupby('customer_unique_id').size()
        report.append(f"  Affected unique customers: {len(dup_groups)}")
        report.append(f"  Example duplicates: {dup_groups.head(3).to_dict()}")
        df_customers = df_customers.drop_duplicates(subset=['customer_unique_id'], keep='first')

    if all(col in df_customers.columns for col in ['customer_city', 'customer_zip_code_prefix']):
        df_customers['city_normalized'] = df_customers['customer_city'].str.lower().str.strip()
        potential_dupes = df_customers.duplicated(
            subset=['city_normalized', 'customer_zip_code_prefix'],
            keep=False
        )

        if potential_dupes.any():
            pot_count = potential_dupes.sum()
            report.append(f"Potential duplicates by city+zip: {pot_count} records")
            sample_dupes = df_customers[potential_dupes].head(5)
            for _, row in sample_dupes.iterrows():
                report.append(f"  Example: {row['customer_city']} {row['customer_zip_code_prefix']}")

    if 'city_normalized' in df_customers.columns:
        df_customers = df_customers.drop(columns=['city_normalized'])

    final_count = len(df_customers)
    removed = initial_count - final_count
    report.append(f"Removed duplicates: {removed}")
    report.append(f"Final customer count: {final_count}")

    report_path = DATA_DIR / "deduplication_report.txt"
    with open(report_path, 'w') as f:
        f.write("\n".join(report))
    print(f"Report saved: {report_path}")

    return df_customers

def deduplicate_sellers(df_sellers):
    if df_sellers is None or df_sellers.empty:
        return df_sellers

    print("Deduplicating sellers...")
    initial_count = len(df_sellers)
    df_sellers = df_sellers.drop_duplicates(subset=['seller_id'], keep='first')
    final_count = len(df_sellers)
    removed = initial_count - final_count
    print(f"  Removed seller duplicates: {removed}")

    return df_sellers


def transform_orders(df_orders, valid_customer_ids=None):
    df_orders['order_purchase_timestamp'] = pd.to_datetime(df_orders['order_purchase_timestamp'], errors='coerce')

    # Нормализация статусов
    df_orders['order_status'] = df_orders['order_status'].str.lower().fillna('unknown')

    # Маппинг статусов к стандартным значениям
    status_mapping = {
        'canceled': 'cancelled',
        'processing': 'approved',
        'invoiced': 'approved',
        'created': 'approved',
        'unavailable': 'unknown'
    }

    df_orders['order_status'] = df_orders['order_status'].apply(
        lambda x: status_mapping.get(x, x)
    )

    # Удаление дубликатов
    df_orders = df_orders.drop_duplicates(subset=['order_id'])

    # Расчёт времени доставки
    if 'order_delivered_customer_date' in df_orders.columns:
        df_orders['order_delivered_customer_date'] = pd.to_datetime(
            df_orders['order_delivered_customer_date'], errors='coerce'
        )
        df_orders['delivery_time_days'] = (
                df_orders['order_delivered_customer_date'] - df_orders['order_purchase_timestamp']
        ).dt.days.fillna(0).astype(int)

    # Проверка FK: удаление заказов с несуществующими клиентами
    if valid_customer_ids is not None:
        invalid_mask = ~df_orders['customer_id'].isin(valid_customer_ids)
        if invalid_mask.any():
            invalid_count = invalid_mask.sum()
            print(f"  Removing {invalid_count} orders with invalid customer_id")
            if invalid_count <= 10:
                print(
                    f"    Example invalid customer_ids: {df_orders[invalid_mask]['customer_id'].unique()[:5].tolist()}")
            df_orders = df_orders[~invalid_mask]

    return df_orders

def transform_items(df_items):
    if 'price' in df_items.columns:
        bad_prices = df_items['price'] <= 0
        if bad_prices.any():
            print(f"  Removed records with price <= 0: {bad_prices.sum()}")
            df_items = df_items[~bad_prices]

    if 'freight_value' in df_items.columns:
        bad_freight = (df_items['freight_value'] < 0) | (df_items['freight_value'] > 1000)
        if bad_freight.any():
            print(f"  Removed records with abnormal freight_value: {bad_freight.sum()}")
            df_items = df_items[~bad_freight]

    return df_items

def upsert_sqlite(df_orders, df_customers, df_products, df_items, df_sellers=None):
    conn = sqlite3.connect(DB_PATH)

    df_orders.to_sql("fact_orders", conn, if_exists="replace", index=False)
    df_items.to_sql("fact_order_items", conn, if_exists="replace", index=False)
    df_customers.to_sql("dim_customers", conn, if_exists="replace", index=False)
    df_products.to_sql("dim_products", conn, if_exists="replace", index=False)

    if df_sellers is not None and not df_sellers.empty:
        df_sellers.to_sql("dim_sellers", conn, if_exists="replace", index=False)

    conn.close()


def main():
    print("\n1. Loading data...")
    orders = load_csv("olist_orders.csv")
    customers = load_csv("olist_customers.csv")
    products = load_csv("olist_products.csv")
    items = load_csv("olist_order_items.csv")

    try:
        sellers = load_csv("olist_sellers.csv")
        print(f"   Sellers loaded: {len(sellers)}")
    except FileNotFoundError:
        print("   Sellers file not found, skipping")
        sellers = None

    print(f"\n2. Initial sizes:")
    print(f"   Orders: {orders.shape}")
    print(f"   Customers: {customers.shape}")
    print(f"   Products: {products.shape}")
    print(f"   Order items: {items.shape}")

    print("\n3. Cleaning and deduplication...")
    customers = deduplicate_customers(customers)
    if sellers is not None:
        sellers = deduplicate_sellers(sellers)

    print("\n4. Data transformation...")

    # Получаем список валидных customer_id после дедупликации
    valid_customer_ids = customers['customer_id'].unique()

    # Передаём valid_customer_ids в transform_orders
    orders = transform_orders(orders, valid_customer_ids)
    items = transform_items(items)

    print(f"\n5. Final sizes:")
    print(f"   Orders: {orders.shape}")
    print(f"   Customers: {customers.shape}")
    print(f"   Products: {products.shape}")
    print(f"   Order items: {items.shape}")

    print("\n6. Saving to SQLite...")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    upsert_sqlite(orders, customers, products, items, sellers)

if __name__ == "__main__":
    main()