import sqlite3
import pandas as pd

DB_PATH = "data/ecommerce.db"

def main():
    try:
        conn = sqlite3.connect(DB_PATH)
    except FileNotFoundError:
        print(f"Database not found at {DB_PATH}. Run ETL first.")
        return

    # Загружаем данные
    orders = pd.read_sql("SELECT * FROM fact_orders", conn)

    # GMV = сумма всех заказов
    gmv = orders['order_total'].sum()

    # Число заказов
    num_orders = len(orders)

    # Число уникальных покупателей
    num_customers = orders['customer_id'].nunique()

    print("===== E-COMMERCE METRICS SUMMARY =====")
    print(f"GMV: {gmv}")
    print(f"Total Orders: {num_orders}")
    print(f"Unique Customers: {num_customers}")
    print("=====================================")

if __name__ == "__main__":
    main()
