from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

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

def check_kafka_connect():
    """Проверка доступности Kafka Connect"""
    import requests
    try:
        response = requests.get('http://kafka-connect:8083/connectors', timeout=10)
        if response.status_code == 200:
            print("Kafka Connect is ready!")
            return True
        else:
            raise Exception(f"Kafka Connect returned status: {response.status_code}")
    except Exception as e:
        raise Exception(f"Kafka Connect check failed: {str(e)}")

with DAG(
    'data_processing_pipeline',
    default_args=default_args,
    description='Complete ETL/ELT pipeline from source to analytics',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['data', 'etl', 'analytics']
) as dag:

    # 1. Проверка доступности сервисов
    check_services = PythonOperator(
        task_id='check_kafka_connect',
        python_callable=check_kafka_connect
    )

    # 2. Запуск Spark Streaming job
    start_spark_streaming = BashOperator(
        task_id='start_spark_streaming',
        bash_command="""
        /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark:7077 \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
        --conf spark.sql.catalog.spark_catalog.type=hadoop \
        --conf spark.sql.catalog.spark_catalog.warehouse=s3a://warehouse/ \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        /opt/bitnami/spark/jobs/streaming_processor.py
        """
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
    load_to_clickhouse = BashOperator(
        task_id='load_to_clickhouse',
        bash_command="""
        /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark:7077 \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.0,com.clickhouse:clickhouse-jdbc:0.4.6 \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
        --conf spark.sql.catalog.spark_catalog.type=hadoop \
        --conf spark.sql.catalog.spark_catalog.warehouse=s3a://warehouse/ \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        /opt/bitnami/spark/jobs/clickhouse_loader.py
        """
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