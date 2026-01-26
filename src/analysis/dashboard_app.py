import sqlite3
import pandas as pd
from dash import Dash, dcc, html, Input, Output
import plotly.express as px
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "ecommerce.db"

app = Dash(__name__, title="E-commerce Analytics Dashboard")
app.config.suppress_callback_exceptions = True


def load_sales_data():
    conn = sqlite3.connect(DB_PATH)

    daily_sales = pd.read_sql("""
        SELECT order_date, SUM(revenue) as revenue, SUM(orders_count) as orders_count
        FROM mart_daily_category 
        WHERE order_date IS NOT NULL
        GROUP BY order_date
        ORDER BY order_date
    """, conn)

    top_categories = pd.read_sql("""
        SELECT product_category_name, total_revenue as revenue, orders_count
        FROM mart_product_performance
        WHERE product_category_name IS NOT NULL
        ORDER BY total_revenue DESC
        LIMIT 15
    """, conn)

    city_sales = pd.read_sql("""
        SELECT customer_city, SUM(revenue) as revenue, SUM(orders_count) as orders_count
        FROM mart_weekly_city
        WHERE customer_city IS NOT NULL
        GROUP BY customer_city
        ORDER BY revenue DESC
        LIMIT 15
    """, conn)

    overall = pd.read_sql("""
        SELECT 
            COUNT(DISTINCT o.order_id) as total_orders,
            COUNT(DISTINCT o.customer_id) as total_customers,
            SUM(i.price + i.freight_value) as gmv,
            AVG(i.price + i.freight_value) as aov
        FROM fact_orders o
        JOIN fact_order_items i ON o.order_id = i.order_id
    """, conn)

    delivery = pd.read_sql("""
        SELECT 
            AVG(delivery_time_days) as avg_delivery_days,
            SUM(CASE WHEN delivery_time_days > 30 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as late_delivery_rate
        FROM fact_orders
        WHERE delivery_time_days IS NOT NULL
    """, conn)

    conn.close()

    return {
        'daily_sales': daily_sales,
        'top_categories': top_categories,
        'city_sales': city_sales,
        'overall': overall,
        'delivery': delivery
    }

def load_cohort_data():
    try:
        cohort_df = pd.read_csv(Path(__file__).resolve().parents[2] / "docs" / "cohort_retention_data.csv")
        return cohort_df
    except:
        return pd.DataFrame()


def load_sla_data():
    try:
        sla_df = pd.read_csv(Path(__file__).resolve().parents[2] / "docs" / "sla_analysis_results.csv")
        return sla_df
    except:
        return pd.DataFrame()


data = load_sales_data()
cohort_data = load_cohort_data()
sla_data = load_sla_data()

sales_layout = html.Div([
    html.H1("📊 Sales Dashboard", style={'textAlign': 'center'}),

    # KPI Cards
    html.Div([
        html.Div([
            html.H3(f"R${data['overall']['gmv'].iloc[0]:,.0f}"),
            html.P("GMV (Gross Merchandise Value)")
        ], style={'background': '#2E86AB', 'color': 'white', 'padding': '20px', 'borderRadius': '10px', 'textAlign': 'center'}),

        html.Div([
            html.H3(f"R${data['overall']['aov'].iloc[0]:,.2f}"),
            html.P("AOV (Average Order Value)")
        ], style={'background': '#A23B72', 'color': 'white', 'padding': '20px', 'borderRadius': '10px', 'textAlign': 'center'}),

        html.Div([
            html.H3(f"{data['overall']['total_orders'].iloc[0]:,}"),
            html.P("Total Orders")
        ], style={'background': '#F18F01', 'color': 'white', 'padding': '20px', 'borderRadius': '10px', 'textAlign': 'center'}),

        html.Div([
            html.H3(f"{data['overall']['total_customers'].iloc[0]:,}"),
            html.P("Unique Customers")
        ], style={'background': '#C73E1D', 'color': 'white', 'padding': '20px', 'borderRadius': '10px', 'textAlign': 'center'}),
    ], style={'display': 'flex', 'justifyContent': 'space-around', 'flexWrap': 'wrap', 'gap': '20px', 'margin': '30px 0'}),

    # Row 1: Daily trends
    html.Div([
        dcc.Graph(
            figure=px.line(
                data['daily_sales'],
                x='order_date', y='revenue',
                title='Daily Revenue Trend',
                labels={'revenue': 'Revenue (R$)', 'order_date': 'Date'}
            ).update_layout(height=400)
        ),
        dcc.Graph(
            figure=px.line(
                data['daily_sales'],
                x='order_date', y='orders_count',
                title='Daily Orders Trend',
                labels={'orders_count': 'Orders', 'order_date': 'Date'}
            ).update_layout(height=400)
        ),
    ], style={'display': 'flex', 'gap': '20px', 'margin': '20px 0'}),

    # Row 2: Top categories and cities
    html.Div([
        dcc.Graph(
            figure=px.bar(
                data['top_categories'].head(10),
                x='revenue', y='product_category_name',
                orientation='h',
                title='Top 10 Categories by Revenue',
                labels={'revenue': 'Revenue (R$)', 'product_category_name': 'Category'}
            ).update_layout(height=400, yaxis={'categoryorder': 'total ascending'})
        ),
        dcc.Graph(
            figure=px.bar(
                data['city_sales'].head(10),
                x='revenue', y='customer_city',
                orientation='h',
                title='Top 10 Cities by Revenue',
                labels={'revenue': 'Revenue (R$)', 'customer_city': 'City'}
            ).update_layout(height=400, yaxis={'categoryorder': 'total ascending'})
        ),
    ], style={'display': 'flex', 'gap': '20px', 'margin': '20px 0'}),
])

# Cohorts page layout
cohorts_layout = html.Div([
    html.H1("Cohort Analysis", style={'textAlign': 'center'}),

    html.Div([
        html.P("Analysis shows 0% retention as all customers make only one purchase.",
               style={'textAlign': 'center', 'color': '#666', 'fontStyle': 'italic'})
    ]),

    html.Div([
        html.H3("Cohort Retention Data"),
        html.Table([
            html.Thead(html.Tr([html.Th(col) for col in cohort_data.columns])),
            html.Tbody([
                html.Tr([html.Td(cohort_data.iloc[i][col]) for col in cohort_data.columns])
                for i in range(min(len(cohort_data), 10))
            ])
        ], style={'width': '100%', 'borderCollapse': 'collapse'})
    ], style={'marginTop': '20px', 'padding': '20px', 'background': '#f5f5f5'}),
])

# Logistics page layout
logistics_layout = html.Div([
    html.H1("Logistics & SLA", style={'textAlign': 'center'}),

    # KPI Cards for logistics
    html.Div([
        html.Div([
            html.H3(f"{data['delivery']['avg_delivery_days'].iloc[0]:.1f}"),
            html.P("Avg Delivery Days")
        ], style={'background': '#2E86AB', 'color': 'white', 'padding': '20px', 'borderRadius': '10px', 'textAlign': 'center'}),

        html.Div([
            html.H3(f"{data['delivery']['late_delivery_rate'].iloc[0]:.1f}%"),
            html.P("Late Delivery Rate (>30 days)")
        ], style={'background': '#A23B72', 'color': 'white', 'padding': '20px', 'borderRadius': '10px', 'textAlign': 'center'}),
    ], style={'display': 'flex', 'justifyContent': 'center', 'gap': '40px', 'margin': '30px 0'}),

    # SLA Analysis
    html.Div([
        html.H2("Delivery Analysis"),
        html.P("Based on data from SLA analysis")
    ], style={'textAlign': 'center', 'margin': '20px 0'}),
])

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),

    # Navigation
    html.Div([
        dcc.Link('📊 Sales', href='/', style={'color': 'white', 'textDecoration': 'none', 'fontSize': '16px', 'fontWeight': 'bold', 'padding': '10px 20px'}),
        dcc.Link('📈 Cohorts', href='/cohorts', style={'color': 'white', 'textDecoration': 'none', 'fontSize': '16px', 'fontWeight': 'bold', 'padding': '10px 20px'}),
        dcc.Link('Logistics', href='/logistics', style={'color': 'white', 'textDecoration': 'none', 'fontSize': '16px', 'fontWeight': 'bold', 'padding': '10px 20px'}),
    ], style={'backgroundColor': '#2c3e50', 'padding': '15px', 'display': 'flex', 'justifyContent': 'center', 'gap': '30px'}),

    # Page content
    html.Div(id='page-content', style={'padding': '20px'}),
])


# Callback to switch pages
@app.callback(
    Output('page-content', 'children'),
    [Input('url', 'pathname')]
)
def display_page(pathname):
    if pathname == '/cohorts':
        return cohorts_layout
    elif pathname == '/logistics':
        return logistics_layout
    else:
        return sales_layout


if __name__ == "__main__":
    print("Start")

    print("Sales Page: http://127.0.0.1:8050/")
    print("Cohorts Page: http://127.0.0.1:8050/cohorts")
    print("Logistics Page: http://127.0.0.1:8050/logistics")

    app.run(debug=True, host="127.0.0.1", port=8050)