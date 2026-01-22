import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

DB = Path(__file__).resolve().parents[2] / "data" / "ecommerce.db"
OUT = Path(__file__).resolve().parents[2] / "docs" / "dashboard_sla.png"

def main():
    conn = sqlite3.connect(DB)
    df = pd.read_sql("SELECT customer_city, delivery_time_days, promised_time_days FROM fact_orders", conn)
    conn.close()
    if df.empty:
        print("No orders in DB — run ETL first.")
        return
    df['delayed'] = df['delivery_time_days'] > df['promised_time_days']
    agg = df.groupby('customer_city').agg({'delayed': 'mean', 'delivery_time_days':'median'}).reset_index()
    print(agg.head())

    plt.figure(figsize=(8,4))
    plt.bar(agg['customer_city'], agg['delayed'])
    plt.xticks(rotation=45, ha='right')
    plt.title("Share of delayed deliveries by city")
    plt.tight_layout()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT)
    print("Saved SLA chart to", OUT)

if __name__ == "__main__":
    main()
