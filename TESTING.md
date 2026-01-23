# Testing plan

- Unit: функции transform_orders — тесты на удаление дублей и фильтрации price
- Integration: ETL -> SQLite: проверить таблицы созданы и строки > 0
- Data quality: PK uniqueness, FK customer_id exists, allowed statuses
- End-to-End: run DAG mock -> ETL -> quality -> analytics produce PNGs
