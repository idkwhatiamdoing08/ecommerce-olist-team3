"""
SLA (Service Level Agreement) analysis for delivery performance.
Calculates late delivery rates and median delivery time by city/category.
"""
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import seaborn as sns

DB = Path(__file__).resolve().parents[2] / "data" / "ecommerce.db"
OUT_CHART_CITY = Path(__file__).resolve().parents[2] / "docs" / "sla_city_analysis.png"
OUT_CHART_CATEGORY = Path(__file__).resolve().parents[2] / "docs" / "sla_category_analysis.png"
OUT_DATA = Path(__file__).resolve().parents[2] / "docs" / "sla_analysis_results.csv"


def calculate_sla_metrics(conn):
    """
    Calculate SLA metrics: late delivery rate and median delivery time
    by city and product category.
    """
    # Основные метрики по городам
    query_city = """
    SELECT 
        c.customer_city,
        COUNT(o.order_id) AS total_orders,
        SUM(CASE WHEN o.delivery_time_days > 30 THEN 1 ELSE 0 END) AS late_orders,
        AVG(o.delivery_time_days) AS avg_delivery_days,
        AVG(CASE 
            WHEN o.delivery_time_days IS NOT NULL 
            THEN o.delivery_time_days 
            ELSE NULL 
        END) AS avg_delivery_days_not_null
    FROM fact_orders o
    JOIN dim_customers c ON o.customer_id = c.customer_id
    WHERE o.delivery_time_days IS NOT NULL
      AND c.customer_city IS NOT NULL
    GROUP BY c.customer_city
    HAVING COUNT(o.order_id) >= 10  -- Фильтр для статистической значимости
    ORDER BY total_orders DESC
    """

    df_city = pd.read_sql_query(query_city, conn)

    if not df_city.empty:
        df_city['late_delivery_rate'] = (df_city['late_orders'] / df_city['total_orders'] * 100).round(2)
        df_city['avg_delivery_days'] = df_city['avg_delivery_days'].round(2)

    # Метрики по категориям
    query_category = """
    SELECT 
        p.product_category_name,
        COUNT(o.order_id) AS total_orders,
        SUM(CASE WHEN o.delivery_time_days > 30 THEN 1 ELSE 0 END) AS late_orders,
        AVG(o.delivery_time_days) AS avg_delivery_days
    FROM fact_orders o
    JOIN fact_order_items i ON o.order_id = i.order_id
    JOIN dim_products p ON i.product_id = p.product_id
    WHERE o.delivery_time_days IS NOT NULL
      AND p.product_category_name IS NOT NULL
    GROUP BY p.product_category_name
    HAVING COUNT(o.order_id) >= 5
    ORDER BY total_orders DESC
    """

    df_category = pd.read_sql_query(query_category, conn)

    if not df_category.empty:
        df_category['late_delivery_rate'] = (df_category['late_orders'] / df_category['total_orders'] * 100).round(2)
        df_category['avg_delivery_days'] = df_category['avg_delivery_days'].round(2)

    return df_city, df_category


def plot_city_sla(df_city, output_path):
    """Plot city-level SLA analysis"""
    if df_city.empty:
        print("⚠️  No city data for SLA analysis")
        return

    # Топ-15 городов по количеству заказов
    top_cities = df_city.head(15).copy()

    fig, axes = plt.subplots(2, 1, figsize=(14, 10))

    # График 1: Доля опозданий
    ax1 = axes[0]
    bars1 = ax1.barh(top_cities['customer_city'], top_cities['late_delivery_rate'])
    ax1.set_xlabel('Late Delivery Rate (%)')
    ax1.set_title('Top 15 Cities by Late Delivery Rate', fontsize=14, pad=20)
    ax1.invert_yaxis()  # Самый высокий вверху

    # Добавляем значения на столбцы
    for bar in bars1:
        width = bar.get_width()
        ax1.text(width + 0.5, bar.get_y() + bar.get_height() / 2,
                 f'{width:.1f}%', va='center', fontsize=9)

    # График 2: Среднее время доставки
    ax2 = axes[1]
    bars2 = ax2.barh(top_cities['customer_city'], top_cities['avg_delivery_days'])
    ax2.set_xlabel('Average Delivery Time (days)')
    ax2.set_title('Top 15 Cities by Average Delivery Time', fontsize=14, pad=20)
    ax2.invert_yaxis()

    # Добавляем значения на столбцы
    for bar in bars2:
        width = bar.get_width()
        ax2.text(width + 0.5, bar.get_y() + bar.get_height() / 2,
                 f'{width:.1f}d', va='center', fontsize=9)

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    plt.close()


def plot_category_sla(df_category, output_path):
    """Plot category-level SLA analysis"""
    if df_category.empty:
        print("⚠️  No category data for SLA analysis")
        return

    # Топ-15 категорий по количеству заказов
    top_categories = df_category.head(15).copy()

    fig, axes = plt.subplots(2, 1, figsize=(14, 10))

    # График 1: Доля опозданий по категориям
    ax1 = axes[0]
    bars1 = ax1.barh(top_categories['product_category_name'], top_categories['late_delivery_rate'])
    ax1.set_xlabel('Late Delivery Rate (%)')
    ax1.set_title('Top 15 Categories by Late Delivery Rate', fontsize=14, pad=20)
    ax1.invert_yaxis()

    for bar in bars1:
        width = bar.get_width()
        ax1.text(width + 0.5, bar.get_y() + bar.get_height() / 2,
                 f'{width:.1f}%', va='center', fontsize=9)

    # График 2: Среднее время доставки по категориям
    ax2 = axes[1]
    bars2 = ax2.barh(top_categories['product_category_name'], top_categories['avg_delivery_days'])
    ax2.set_xlabel('Average Delivery Time (days)')
    ax2.set_title('Top 15 Categories by Average Delivery Time', fontsize=14, pad=20)
    ax2.invert_yaxis()

    for bar in bars2:
        width = bar.get_width()
        ax2.text(width + 0.5, bar.get_y() + bar.get_height() / 2,
                 f'{width:.1f}d', va='center', fontsize=9)

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    plt.close()


def calculate_overall_sla(conn):
    """Calculate overall SLA metrics"""
    query = """
    SELECT 
        COUNT(*) AS total_orders,
        SUM(CASE WHEN delivery_time_days > 30 THEN 1 ELSE 0 END) AS late_orders,
        AVG(delivery_time_days) AS avg_delivery_days,
        AVG(CASE 
            WHEN delivery_time_days IS NOT NULL 
            THEN delivery_time_days 
            ELSE NULL 
        END) AS avg_delivery_days_not_null
    FROM fact_orders
    WHERE delivery_time_days IS NOT NULL
    """

    result = pd.read_sql_query(query, conn)

    if not result.empty:
        total = result['total_orders'].iloc[0]
        late = result['late_orders'].iloc[0]
        avg_days = result['avg_delivery_days_not_null'].iloc[0]

        late_rate = (late / total * 100) if total > 0 else 0

        return {
            'total_orders_with_delivery': int(total),
            'late_orders': int(late),
            'late_delivery_rate': round(late_rate, 2),
            'avg_delivery_days': round(avg_days, 2) if avg_days else 0
        }

    return None


def main():
    conn = sqlite3.connect(DB)

    print("=" * 60)
    print("SLA (DELIVERY PERFORMANCE) ANALYSIS")
    print("=" * 60)

    overall = calculate_overall_sla(conn)

    if overall:
        print(f"Total orders with delivery data: {overall['total_orders_with_delivery']:,}")
        print(f"Late deliveries (>30 days): {overall['late_orders']:,}")
        print(f"Late delivery rate: {overall['late_delivery_rate']}%")
        print(f"Average delivery time: {overall['avg_delivery_days']} days")

    df_city, df_category = calculate_sla_metrics(conn)

    print("\n🏙️  CITY-LEVEL ANALYSIS (Top 10):")
    print("-" * 40)
    if not df_city.empty:
        print(df_city.head(10).to_string(index=False))
    else:
        print("No city data available")

    print("\n📦 CATEGORY-LEVEL ANALYSIS (Top 10):")
    print("-" * 40)
    if not df_category.empty:
        print(df_category.head(10).to_string(index=False))
    else:
        print("No category data available")

    # Визуализация
    plot_city_sla(df_city, OUT_CHART_CITY)
    print(f"\n📊 City SLA chart saved to: {OUT_CHART_CITY}")

    plot_category_sla(df_category, OUT_CHART_CATEGORY)
    print(f"📊 Category SLA chart saved to: {OUT_CHART_CATEGORY}")

    # Сохраняем сводные данные
    if not df_city.empty and not df_category.empty:
        summary = {
            'overall_metrics': overall,
            'top_cities': df_city.head(20).to_dict('records'),
            'top_categories': df_category.head(20).to_dict('records')
        }

        combined_df = pd.concat([
            df_city.assign(level='city'),
            df_category.assign(level='category')
        ], ignore_index=True)
        combined_df.to_csv(OUT_DATA, index=False)
        print(f"Detailed data saved to: {OUT_DATA}")

    conn.close()

    print("THE END")



if __name__ == "__main__":
    main()