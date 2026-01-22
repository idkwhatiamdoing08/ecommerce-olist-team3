import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

DB = Path(__file__).resolve().parents[2] / "data" / "ecommerce.db"
OUT = Path(__file__).resolve().parents[2] / "docs" / "dashboard_rfm.png"

def main():
    conn = sqlite3.connect(DB)
    df = pd.read_sql("SELECT order_id, customer_id, order_purchase_timestamp, price FROM fact_orders", conn, parse_dates=['order_purchase_timestamp'])
    conn.close()
    if df.empty:
        print("No orders in DB — run ETL first.")
        return
    snapshot = pd.Timestamp('2023-04-01')
    rfm = df.groupby('customer_id').agg({
        'order_purchase_timestamp': lambda x: (snapshot - x.max()).days,
        'order_id': 'count',
        'price': 'sum'
    }).rename(columns={'order_purchase_timestamp':'Recency','order_id':'Frequency','price':'Monetary'}).reset_index()
    print(rfm.head())

    # very simple segmentation
    rfm['Segment'] = ['High' if m>300 else 'Low' for m in rfm['Monetary']]

    counts = rfm['Segment'].value_counts()
    counts.plot(kind='bar')
    plt.title("RFM segments (simple)")
    plt.tight_layout()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT)
    print("Saved RFM chart to", OUT)

if __name__ == "__main__":
    main()
