import sqlite3
import pandas as pd
from dash import Dash, dcc, html
import plotly.express as px

DB_PATH = "data/ecommerce.db"

# Загружаем данные
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM fact_orders", conn, parse_dates=['order_purchase_timestamp'])
conn.close()

# Преобразуем даты в месяцы и в строку
df['cohort_month'] = df['order_purchase_timestamp'].dt.to_period('M').astype(str)

# Считаем уникальных покупателей по месяцу
cohort = df.groupby('cohort_month')['customer_id'].nunique().reset_index()
cohort.rename(columns={'customer_id': 'unique_customers'}, inplace=True)

# Создаём график
fig = px.line(cohort, x='cohort_month', y='unique_customers', title="Monthly unique customers (cohort)")

# Dash app
app = Dash(__name__)
app.layout = html.Div([
    html.H1("E-commerce Dashboard"),
    dcc.Graph(figure=fig)
])

if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=8050)
