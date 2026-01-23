# Архитектура (star schema & pipeline)

CSV (data/) --> src/etl/etl_pipeline.py --> SQLite (data/ecommerce.db)
Data mart: fact_orders, fact_order_items, dim_customers, dim_product, dim_geography, dim_calendar

Orchestration: mock DAG in src/airflow_dag/ecommerce_etl_dag.py
Data quality checks: src/etl/data_quality_checks.py
Analytics: src/analysis/\*.py (cohort, rfm, sla)
