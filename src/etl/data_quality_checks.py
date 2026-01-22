import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "ecommerce.db"

def check_pk_uniqueness(conn):
    q = "SELECT order_id, COUNT(*) c FROM fact_orders GROUP BY order_id HAVING c>1"
    cur = conn.execute(q).fetchall()
    return cur

def check_fk_customer(conn):
    q = """
    SELECT f.order_id
    FROM fact_orders f
    LEFT JOIN dim_customers c ON f.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
    LIMIT 5
    """
    return conn.execute(q).fetchall()

def check_allowed_statuses(conn):
    q = "SELECT DISTINCT order_status FROM fact_orders"
    return [r[0] for r in conn.execute(q).fetchall()]

def main():
    if not DB_PATH.exists():
        print("DB not found. Run ETL first.")
        return
    conn = sqlite3.connect(DB_PATH)
    dupes = check_pk_uniqueness(conn)
    if dupes:
        print("❌ Duplicate PKs found:", dupes[:5])
    else:
        print("✅ PK uniqueness OK")

    missing_fk = check_fk_customer(conn)
    if missing_fk:
        print("❌ FK missing for orders:", missing_fk[:5])
    else:
        print("✅ FK integrity OK (customer_id)")

    statuses = check_allowed_statuses(conn)
    allowed = {'delivered','cancelled','approved','shipped','unknown'}
    bad = set(statuses) - allowed
    if bad:
        print("❌ Unexpected statuses:", bad)
    else:
        print("✅ Status values OK:", statuses)

    conn.close()

if __name__ == "__main__":
    main()
