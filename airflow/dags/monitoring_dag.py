from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor

def check_data_freshness():
    """Проверка свежести данных"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    hook = PostgresHook(postgres_conn_id='source_db')
    records = hook.get_records("""
        SELECT MAX(created_at) as latest_record 
        FROM orders 
        WHERE created_at >= NOW() - INTERVAL '2 hours'
    """)
    
    if not records or not records[0][0]:
        raise Exception("Data is not fresh! No recent records found.")
    
    print(f"Latest record: {records[0][0]}")
    return True

def check_row_counts():
    """Проверка количества строк в ключевых таблицах"""
    from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook
    
    hook = ClickHouseHook(clickhouse_conn_id='clickhouse_dwh')
    
    checks = {
        'staging.customers': 100,  # минимум 100 записей
        'analytics.dim_customers': 100,
        'analytics.fct_orders': 500
    }
    
    for table, min_count in checks.items():
        result = hook.get_records(f"SELECT COUNT(*) FROM {table}")
        count = result[0][0] if result else 0
        
        if count < min_count:
            raise Exception(f"Table {table} has only {count} rows, expected at least {min_count}")
        
        print(f"✓ {table}: {count} rows")

with DAG(
    'data_monitoring',
    default_args={
        'owner': 'data_team',
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Data quality and freshness monitoring',
    schedule_interval=timedelta(minutes=30),
    catchup=False,
    tags=['monitoring', 'data-quality']
) as dag:

    check_freshness = PythonOperator(
        task_id='check_data_freshness',
        python_callable=check_data_freshness
    )

    check_counts = PythonOperator(
        task_id='check_row_counts',
        python_callable=check_row_counts
    )

    check_freshness >> check_counts