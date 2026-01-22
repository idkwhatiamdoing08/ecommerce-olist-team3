from datetime import datetime, timedelta
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except Exception:
    # Dummy stand-ins so file can run without Airflow installed
    class DAG:
        def __init__(self, *args, **kwargs): pass
    class PythonOperator:
        def __init__(self, *args, **kwargs): pass

import subprocess
import sys

def extract():
    print("TASK extract: listing CSV files in data/")
    import os
    print(os.listdir("data"))

def transform():
    print("TASK transform: executing ETL script")
    subprocess.run([sys.executable, "src/etl/etl_pipeline.py"], check=False)

def load():
    print("TASK load: ensured by ETL (writes to SQLite)")

def quality_check():
    print("TASK quality_check: running data quality checks")
    subprocess.run([sys.executable, "src/etl/data_quality_checks.py"], check=False)

if __name__ == "__main__":
    print("Simulating DAG: extract -> transform -> load -> quality_check")
    extract()
    transform()
    load()
    quality_check()
    print("DAG simulation complete:", datetime.now())
