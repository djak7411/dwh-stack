from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def check_dbt_ready():
    """Проверка готовности dbt проекта"""
    import subprocess
    try:
        result = subprocess.run(['dbt', 'debug'], capture_output=True, text=True)
        if result.returncode == 0:
            print("dbt project is ready!")
            return True
        else:
            raise Exception(f"dbt debug failed: {result.stderr}")
    except Exception as e:
        raise Exception(f"dbt check error: {str(e)}")

def trigger_dbt_run(**context):
    """Запуск dbt трансформаций"""
    import subprocess
    dag_run = context['dag_run']
    
    # Параметры из DAG run
    models = dag_run.conf.get('models', 'all')
    full_refresh = dag_run.conf.get('full_refresh', False)
    
    cmd = ['dbt', 'run']
    if models != 'all':
        cmd.extend(['--models', models])
    if full_refresh:
        cmd.append('--full-refresh')
    
    result = subprocess.run(cmd, capture_output=True, text=True, cwd='/opt/airflow/dbt/analytics_platform')
    
    if result.returncode != 0:
        raise Exception(f"dbt run failed: {result.stderr}")
    
    print("dbt run completed successfully!")
    return True

with DAG(
    'data_processing_pipeline',
    default_args=default_args,
    description='Complete ETL/ELT pipeline from source to analytics',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['data', 'etl', 'analytics']
) as dag:

    # 1. Проверка доступности сервисов
    check_services = HttpSensor(
        task_id='check_kafka_connect',
        http_conn_id='kafka_connect',
        endpoint='/connectors',
        response_check=lambda response: response.status_code == 200,
        timeout=300,
        poke_interval=30
    )

    # 2. Запуск Spark Streaming job
    start_spark_streaming = SparkSubmitOperator(
        task_id='start_spark_streaming',
        application='/opt/spark/jobs/streaming_processor.py',
        conn_id='spark_default',
        application_args=['--config', '/opt/spark/conf/streaming.conf'],
        verbose=True
    )

    # 3. Проверка dbt
    check_dbt = PythonOperator(
        task_id='check_dbt_ready',
        python_callable=check_dbt_ready
    )

    # 4. Запуск dbt трансформаций
    run_dbt_transformations = PythonOperator(
        task_id='run_dbt_transformations',
        python_callable=trigger_dbt_run,
        provide_context=True
    )

    # 5. Загрузка данных в ClickHouse
    load_to_clickhouse = SparkSubmitOperator(
        task_id='load_to_clickhouse',
        application='/opt/spark/jobs/clickhouse_loader.py',
        conn_id='spark_default',
        verbose=True
    )

    # 6. Запуск тестов качества данных
    run_data_quality_tests = BashOperator(
        task_id='run_data_quality_tests',
        bash_command='cd /opt/airflow/dbt/analytics_platform && dbt test --models tag:data_quality',
        env={'DBT_PROFILES_DIR': '/opt/airflow/dbt'}
    )

    # 7. Генерация документации
    generate_docs = BashOperator(
        task_id='generate_documentation',
        bash_command='cd /opt/airflow/dbt/analytics_platform && dbt docs generate',
        env={'DBT_PROFILES_DIR': '/opt/airflow/dbt'}
    )

    # Определение зависимостей
    check_services >> start_spark_streaming >> check_dbt >> run_dbt_transformations
    run_dbt_transformations >> [load_to_clickhouse, run_data_quality_tests]
    load_to_clickhouse >> generate_docs