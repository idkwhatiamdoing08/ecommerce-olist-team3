"""
Cohort / retention visualization.
Reads fact_orders from SQLite and writes PNG to docs/.
"""
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

DB = Path(__file__).resolve().parents[2] / "data" / "ecommerce.db"
OUT = Path(__file__).resolve().parents[2] / "docs" / "dashboard_cohort.png"

def main():
    conn = sqlite3.connect(DB)
    df = pd.read_sql("SELECT * FROM fact_orders", conn, parse_dates=['order_purchase_timestamp'])
    conn.close()
    if df.empty:
        print("No orders in DB — run ETL first.")
        return
    df['cohort_month'] = df['order_purchase_timestamp'].dt.to_period('M')
    cohort = df.groupby('cohort_month').agg({'customer_id': pd.Series.nunique}).rename(columns={'customer_id':'n_customers'}).reset_index()
    print(cohort.head())

    plt.figure(figsize=(8,4))
    plt.plot(cohort['cohort_month'].astype(str), cohort['n_customers'], marker='o')
    plt.title("Monthly unique customers (cohort month)")
    plt.xlabel("Cohort month")
    plt.ylabel("Unique customers")
    plt.grid(True)
    plt.tight_layout()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT)
    print("Saved cohort chart to", OUT)

if __name__ == "__main__":
    main()
