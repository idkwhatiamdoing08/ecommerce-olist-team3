import pandas as pd
import os

data_path = "../data/orders.csv"


if os.path.exists(data_path):
    orders = pd.read_csv(data_path)
else:
    orders = pd.DataFrame({
        "order_id": [],
        "customer_id": [],
        "order_status": [],
        "order_purchase_timestamp": [],
        "order_delivered_timestamp": []
    })


orders['order_status'] = orders['order_status'].str.lower()
orders.drop_duplicates(inplace=True)


summary = orders.groupby('order_status').size()
print(summary)
