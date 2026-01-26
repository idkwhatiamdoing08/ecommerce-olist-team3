"""
Cohort retention analysis with 1/2/3 month retention rates.
"""
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import numpy as np

DB = Path(__file__).resolve().parents[2] / "data" / "ecommerce.db"
OUT_CHART = Path(__file__).resolve().parents[2] / "docs" / "cohort_retention_chart.png"
OUT_DATA = Path(__file__).resolve().parents[2] / "docs" / "cohort_retention_data.csv"


def calculate_cohort_retention(conn):
    """
    Calculate cohort retention for 1, 2, 3 months.
    Returns DataFrame with cohort_month, cohort_size, retention rates.
    """
    query = """
    SELECT 
        customer_id,
        DATE(order_purchase_timestamp) AS order_date
    FROM fact_orders
    WHERE order_status NOT IN ('cancelled', 'unavailable')
    ORDER BY customer_id, order_date
    """
    df = pd.read_sql_query(query, conn, parse_dates=['order_date'])

    if df.empty:
        print("No orders in database")
        return pd.DataFrame()

    # Определяем когорту (месяц первой покупки)
    df['cohort_month'] = df.groupby('customer_id')['order_date'].transform('min').dt.to_period('M')
    df['order_month'] = df['order_date'].dt.to_period('M')

    # Создаём сводную таблицу
    cohort_data = df.groupby(['cohort_month', 'order_month']).agg(
        n_customers=('customer_id', 'nunique')
    ).reset_index()

    # Вычисляем разницу в месяцах между когортой и месяцем заказа
    cohort_data['months_diff'] = (cohort_data['order_month'] - cohort_data['cohort_month']).apply(lambda x: x.n)

    # Сводная таблица для матрицы удержания
    retention_pivot = cohort_data.pivot_table(
        index='cohort_month',
        columns='months_diff',
        values='n_customers',
        aggfunc='sum'
    )

    # Размер когорты (месяц 0)
    cohort_sizes = retention_pivot[0]

    # Вычисляем проценты удержания
    retention_rates = retention_pivot.div(cohort_sizes, axis=0) * 100

    # Создаём итоговый DataFrame
    result = pd.DataFrame({
        'cohort_month': retention_rates.index.astype(str),
        'cohort_size': cohort_sizes.values
    })

    # Добавляем удержание для 1, 2, 3 месяцев
    for month in [1, 2, 3]:
        col_name = f'retention_month_{month}'
        if month in retention_rates.columns:
            result[col_name] = retention_rates[month].round(2).values
        else:
            result[col_name] = np.nan

    return result, retention_rates


def plot_cohort_retention(retention_rates, output_path):
    """Plot cohort retention heatmap"""
    plt.figure(figsize=(12, 8))

    # Матрица удержания
    data_to_plot = retention_rates.fillna(0)

    # Создаём тепловую карту
    plt.imshow(data_to_plot, aspect='auto', cmap='YlGnBu', interpolation='nearest')
    plt.colorbar(label='Retention Rate (%)')

    # Подписи осей
    plt.title('Cohort Retention Analysis', fontsize=16, pad=20)
    plt.xlabel('Months After Cohort', fontsize=12)
    plt.ylabel('Cohort Month', fontsize=12)

    # Подписи значений
    for i in range(len(data_to_plot)):
        for j in range(len(data_to_plot.columns)):
            value = data_to_plot.iloc[i, j]
            if value > 0:
                plt.text(j, i, f'{value:.1f}%', ha='center', va='center',
                         color='white' if value > 50 else 'black', fontsize=8)

    # Подписи месяцев
    plt.xticks(range(len(data_to_plot.columns)), data_to_plot.columns)
    plt.yticks(range(len(data_to_plot)), data_to_plot.index.astype(str))

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    plt.close()

def check_repeat_customers(conn):
    """Check if customers make repeat purchases"""
    query = """
    SELECT 
        customer_id,
        COUNT(DISTINCT DATE(order_purchase_timestamp)) as purchase_days,
        COUNT(DISTINCT order_id) as orders_count,
        MIN(DATE(order_purchase_timestamp)) as first_purchase,
        MAX(DATE(order_purchase_timestamp)) as last_purchase
    FROM fact_orders
    WHERE order_status NOT IN ('cancelled', 'unavailable')
    GROUP BY customer_id
    HAVING COUNT(DISTINCT order_id) > 1
    ORDER BY orders_count DESC
    LIMIT 10
    """
    repeat_customers = pd.read_sql_query(query, conn)
    return repeat_customers


def main():
    """Main function to calculate and display cohort retention"""
    conn = sqlite3.connect(DB)

    print("=" * 60)
    print("COHORT RETENTION ANALYSIS")
    print("=" * 60)

    # Проверяем повторных покупателей
    repeat_customers = check_repeat_customers(conn)
    print(f"\n🔍 REPEAT CUSTOMERS ANALYSIS:")
    print(f"   Total customers with repeat purchases: {len(repeat_customers)}")
    if len(repeat_customers) > 0:
        print(f"   Sample repeat customers:")
        print(repeat_customers.head(5).to_string(index=False))
    else:
        print("   ⚠️  NO REPEAT CUSTOMERS FOUND!")
        print("   This explains why retention is 0%")

    # Вычисляем удержание
    cohort_df, retention_matrix = calculate_cohort_retention(conn)

    if cohort_df.empty:
        print("No data available for cohort analysis")
        conn.close()
        return

    # Выводим результаты
    print("\n📊 COHORT RETENTION RATES:")
    print("-" * 60)

    # Заменяем NaN на 0% для наглядности
    display_df = cohort_df.copy()
    for col in ['retention_month_1', 'retention_month_2', 'retention_month_3']:
        display_df[col] = display_df[col].fillna(0)

    print(display_df.to_string(index=False))

    # Ключевые метрики
    avg_retention_m1 = cohort_df['retention_month_1'].mean(skipna=True)
    avg_retention_m2 = cohort_df['retention_month_2'].mean(skipna=True)
    avg_retention_m3 = cohort_df['retention_month_3'].mean(skipna=True)

    print("\n📈 AVERAGE RETENTION RATES:")
    print(f"  Month 1: {avg_retention_m1:.2f}%")
    print(f"  Month 2: {avg_retention_m2:.2f}%")
    print(f"  Month 3: {avg_retention_m3:.2f}%")

    # Проверяем, есть ли вообще данные для удержания
    if avg_retention_m1 == 0 and avg_retention_m2 == 0 and avg_retention_m3 == 0:
        print("\n⚠️  WARNING: All retention rates are 0%")
        print("   Possible reasons:")
        print("   1. No repeat customers in dataset")
        print("   2. Dataset time range is too short")
        print("   3. Customers buy only once")

    # Сохраняем данные
    cohort_df.to_csv(OUT_DATA, index=False)
    print(f"\n💾 Data saved to: {OUT_DATA}")

    # Строим график (если есть данные)
    if not retention_matrix.isna().all().all():
        plot_cohort_retention(retention_matrix.fillna(0), OUT_CHART)
        print(f"📊 Chart saved to: {OUT_CHART}")

    conn.close()

    print("\n" + "=" * 60)
    print("✅ COHORT ANALYSIS COMPLETED")
    print("=" * 60)

if __name__ == "__main__":
    main()