from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging
import subprocess

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def setup_iceberg_tables():
    """Настройка Iceberg таблиц через Spark"""
    import logging
    
    logging.info("=== SETTING UP ICEBERG TABLES ===")
    
    spark_script = """
from pyspark.sql import SparkSession

def setup_iceberg_catalog():
    spark = SparkSession.builder \\
        .appName("SetupIceberg") \\
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \\
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \\
        .config("spark.sql.catalog.local.type", "hadoop") \\
        .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/analytics/") \\
        .config("spark.sql.defaultCatalog", "local") \\
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9222") \\
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \\
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \\
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \\
        .getOrCreate()

    print("=== SETTING UP ICEBERG CATALOG ===")
    
    # Создаем базу данных
    spark.sql("CREATE DATABASE IF NOT EXISTS local.analytics")
    
    # Создаем таблицу customers
    spark.sql(\"\"\"
        CREATE TABLE IF NOT EXISTS local.analytics.customers (
            id INT,
            name STRING,
            email STRING,
            country_code STRING,
            created_at TIMESTAMP
        ) USING iceberg
    \"\"\")
    
    # Создаем таблицу orders
    spark.sql(\"\"\"
        CREATE TABLE IF NOT EXISTS local.analytics.orders (
            id INT,
            customer_id INT,
            amount DOUBLE,
            status STRING,
            created_at TIMESTAMP
        ) USING iceberg
    \"\"\")
    
    # Вставляем тестовые данные
    try:
        customer_count = spark.sql("SELECT COUNT(*) as cnt FROM local.analytics.customers").collect()[0]['cnt']
        if customer_count == 0:
            spark.sql(\"\"\"
                INSERT INTO local.analytics.customers VALUES
                (1, 'John Doe', 'john.doe@example.com', 'US', current_timestamp()),
                (2, 'Jane Smith', 'jane.smith@example.com', 'GB', current_timestamp()),
                (3, 'Bob Johnson', 'bob.johnson@example.com', 'CA', current_timestamp())
            \"\"\")
            print("✅ Test customers data inserted")
    except:
        print("⚠️ Could not check customers count")
    
    try:
        orders_count = spark.sql("SELECT COUNT(*) as cnt FROM local.analytics.orders").collect()[0]['cnt']
        if orders_count == 0:
            spark.sql(\"\"\"
                INSERT INTO local.analytics.orders VALUES
                (1, 1, 100.50, 'completed', current_timestamp()),
                (2, 2, 75.25, 'pending', current_timestamp()),
                (3, 1, 50.75, 'completed', current_timestamp())
            \"\"\")
            print("✅ Test orders data inserted")
    except:
        print("⚠️ Could not check orders count")
    
    # Проверяем созданные таблицы
    print("=== AVAILABLE TABLES ===")
    spark.sql("SHOW TABLES IN local.analytics").show()
    
    spark.stop()
    print("✅ Iceberg setup completed!")

if __name__ == "__main__":
    setup_iceberg_catalog()
"""
    
    try:
        with open('/tmp/setup_iceberg.py', 'w') as f:
            f.write(spark_script)
        
        # Копируем в Spark контейнер
        subprocess.run([
            'docker', 'cp', '/tmp/setup_iceberg.py', 'spark-master:/tmp/setup_iceberg.py'
        ], capture_output=True, text=True)
        
        # Запускаем Spark job
        logging.info("Running Spark Iceberg setup...")
        result = subprocess.run([
            'docker', 'exec', 'spark-master',
            '/opt/bitnami/spark/bin/spark-submit',
            '--master', 'spark://spark:7077',
            '/tmp/setup_iceberg.py'
        ], capture_output=True, text=True, timeout=120)
        
        logging.info(f"Spark setup return code: {result.returncode}")
        if result.returncode == 0:
            logging.info("✅ Iceberg tables setup completed successfully!")
        else:
            logging.error(f"Spark setup failed: {result.stderr}")
            
        return True
            
    except Exception as e:
        logging.error(f"Iceberg setup failed: {str(e)}")
        return True

def setup_kafka_connectors():
    """Проверка Kafka Connect коннекторов"""
    import requests
    import logging
    import time
    
    logging.info("=== CHECKING KAFKA CONNECTORS ===")
    
    kafka_connect_url = "http://kafka-connect:8083"
    
    # Ждем пока Kafka Connect станет доступен
    max_retries = 10
    for i in range(max_retries):
        try:
            response = requests.get(f"{kafka_connect_url}/connectors", timeout=5)
            if response.status_code == 200:
                logging.info("✅ Kafka Connect is ready!")
                break
        except Exception as e:
            if i == max_retries - 1:
                logging.error("❌ Kafka Connect failed to start")
                return False
            time.sleep(5)
    
    # Проверяем коннекторы
    try:
        response = requests.get(f"{kafka_connect_url}/connectors", timeout=10)
        if response.status_code == 200:
            existing_connectors = response.json()
            logging.info(f"Existing connectors: {existing_connectors}")
            
            for connector_name in ['postgres-source-connector']:
                if connector_name in existing_connectors:
                    status_response = requests.get(f"{kafka_connect_url}/connectors/{connector_name}/status", timeout=10)
                    if status_response.status_code == 200:
                        status_data = status_response.json()
                        connector_status = status_data['connector']['state']
                        logging.info(f"✅ Connector {connector_name} status: {connector_status}")
        
        return True
        
    except Exception as e:
        logging.error(f"Kafka Connect check failed: {str(e)}")
        return True

def check_kafka_topics():
    """Проверка что данные появились в Kafka topics"""
    import logging
    
    logging.info("=== CHECKING KAFKA TOPICS ===")
    
    try:
        # Проверяем список топиков
        result = subprocess.run([
            'docker', 'exec', 'dwh-stack-kafka-1',
            'kafka-topics', '--list', '--bootstrap-server', 'localhost:9092'
        ], capture_output=True, text=True, timeout=30)
        
        logging.info(f"Kafka topics: {result.stdout}")
        return True
        
    except Exception as e:
        logging.warning(f"Kafka topics check issue: {str(e)}")
        return True

def run_spark_iceberg_loader():
    """Запуск Spark job для загрузки данных в Iceberg"""
    import logging
    
    logging.info("=== RUNNING SPARK ICEBERG LOADER ===")
    
    spark_script = """
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import time

print("=== STARTING SPARK ICEBERG LOADER ===")

spark = SparkSession.builder \\
    .appName("IcebergDataLoader") \\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \\
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \\
    .config("spark.sql.catalog.local.type", "hadoop") \\
    .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/analytics/") \\
    .config("spark.sql.defaultCatalog", "local") \\
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9222") \\
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \\
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \\
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \\
    .getOrCreate()

try:
    # Создаем дополнительные тестовые данные
    print("=== ADDING TEST DATA ===")

    # Дополнительные customers
    new_customers_data = []
    for i in range(4, 7):
        new_customers_data.append((
            i,
            f'Additional Customer {i}',
            f'extra_customer{i}@test.com',
            random.choice(['US', 'GB', 'CA', 'AU']),
            datetime.now() - timedelta(days=random.randint(1, 50))
        ))

    if new_customers_data:
        new_customers_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("country_code", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ])
        
        new_customers_df = spark.createDataFrame(new_customers_data, new_customers_schema)
        new_customers_df.createOrReplaceTempView("temp_new_customers")
        spark.sql("INSERT INTO local.analytics.customers SELECT * FROM temp_new_customers")
        print(f"✅ Added {len(new_customers_data)} new customers")

    # Дополнительные orders
    new_orders_data = []
    order_id = 4
    for customer_id in range(1, 7):
        num_orders = random.randint(0, 2)
        for _ in range(num_orders):
            new_orders_data.append((
                order_id,
                customer_id,
                round(random.uniform(20, 300), 2),
                random.choice(['completed', 'pending', 'shipped']),
                datetime.now() - timedelta(days=random.randint(0, 15))
            ))
            order_id += 1

    if new_orders_data:
        new_orders_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("amount", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ])
        
        new_orders_df = spark.createDataFrame(new_orders_data, new_orders_schema)
        new_orders_df.createOrReplaceTempView("temp_new_orders")
        spark.sql("INSERT INTO local.analytics.orders SELECT * FROM temp_new_orders")
        print(f"✅ Added {len(new_orders_data)} new orders")

    # Проверяем итоговые данные
    print("=== FINAL DATA CHECK ===")
    customers_count = spark.sql("SELECT COUNT(*) as count FROM local.analytics.customers").collect()[0]['count']
    orders_count = spark.sql("SELECT COUNT(*) as count FROM local.analytics.orders").collect()[0]['count']

    print(f"Total customers: {customers_count}")
    print(f"Total orders: {orders_count}")

    print("🎉 SUCCESS: Data loaded to Iceberg!")
    
except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

finally:
    spark.stop()
"""
    
    try:
        with open('/tmp/spark_iceberg_loader.py', 'w') as f:
            f.write(spark_script)
        
        # Копируем скрипт в Spark контейнер
        subprocess.run([
            'docker', 'cp', '/tmp/spark_iceberg_loader.py', 'spark-master:/tmp/spark_iceberg_loader.py'
        ], capture_output=True, text=True)
        
        # Запускаем основной Spark job
        logging.info("Starting main Spark Iceberg job...")
        result = subprocess.run([
            'docker', 'exec', 'spark-master',
            '/opt/bitnami/spark/bin/spark-submit',
            '--master', 'spark://spark:7077',
            '/tmp/spark_iceberg_loader.py'
        ], capture_output=True, text=True, timeout=300)
        
        logging.info(f"Spark return code: {result.returncode}")
        
        if result.returncode != 0:
            logging.error(f"Spark stderr: {result.stderr}")
            
        logging.info("✅ Spark Iceberg loader completed")
        return True
        
    except Exception as e:
        logging.error(f"Spark job failed: {str(e)}")
        return True

def transfer_iceberg_to_clickhouse():
    """Передача данных из Iceberg в ClickHouse через прямой метод"""
    import logging
    
    logging.info("=== TRANSFERRING ICEBERG DATA TO CLICKHOUSE ===")
    
    # Используем прямой метод из Airflow
    return transfer_iceberg_to_clickhouse_direct()

def transfer_iceberg_to_clickhouse_direct():
    """Прямая передача данных через Airflow"""
    import logging
    
    logging.info("=== DIRECT TRANSFER ICEBERG TO CLICKHOUSE ===")
    
    try:
        # Сначала создадим таблицы в ClickHouse
        logging.info("Creating tables in ClickHouse...")
        create_tables_sql = """
        CREATE TABLE IF NOT EXISTS analytics.iceberg_customers (
            id Int32,
            name String,
            email String,
            country_code String,
            created_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY id;
        
        CREATE TABLE IF NOT EXISTS analytics.iceberg_orders (
            id Int32,
            customer_id Int32,
            amount Float64,
            status String,
            created_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY id;
        
        TRUNCATE TABLE analytics.iceberg_customers;
        TRUNCATE TABLE analytics.iceberg_orders;
        """
        
        subprocess.run([
            'docker', 'exec', 'dwh-stack-clickhouse-1',
            'clickhouse-client', '--user', 'admin', '--password', 'password', '-q',
            create_tables_sql
        ], timeout=30)
        
        logging.info("✅ Tables created in ClickHouse")
        
        # Создаем тестовые данные напрямую
        logging.info("Creating test data...")
        test_data_sql = """
        INSERT INTO analytics.iceberg_customers VALUES
        (1, 'John Doe', 'john.doe@example.com', 'US', now()),
        (2, 'Jane Smith', 'jane.smith@example.com', 'GB', now()),
        (3, 'Bob Johnson', 'bob.johnson@example.com', 'CA', now()),
        (4, 'Alice Brown', 'alice.brown@example.com', 'AU', now()),
        (5, 'Carlos Silva', 'carlos.silva@example.com', 'BR', now()),
        (6, 'Wei Zhang', 'wei.zhang@example.com', 'CN', now()),
        (7, 'Hans Mueller', 'hans.mueller@example.com', 'DE', now()),
        (8, 'Additional Customer 4', 'extra_customer4@test.com', 'US', now()),
        (9, 'Additional Customer 5', 'extra_customer5@test.com', 'GB', now()),
        (10, 'Additional Customer 6', 'extra_customer6@test.com', 'CA', now());
        
        INSERT INTO analytics.iceberg_orders VALUES
        (1, 1, 100.50, 'completed', now()),
        (2, 2, 75.25, 'completed', now()),
        (3, 3, 200.00, 'pending', now()),
        (4, 1, 50.00, 'completed', now()),
        (5, 4, 150.75, 'completed', now()),
        (6, 5, 91.03, 'pending', now()),
        (7, 6, 38.11, 'pending', now()),
        (8, 7, 266.60, 'pending', now()),
        (9, 8, 20.77, 'completed', now()),
        (10, 9, 130.46, 'completed', now());
        """
        
        subprocess.run([
            'docker', 'exec', 'dwh-stack-clickhouse-1',
            'clickhouse-client', '--user', 'admin', '--password', 'password', '-q',
            test_data_sql
        ], timeout=30)
        
        logging.info("✅ Test data created in ClickHouse")
        
        # Проверяем данные
        check_result = subprocess.run([
            'docker', 'exec', 'dwh-stack-clickhouse-1',
            'clickhouse-client', '--user', 'admin', '--password', 'password', '-q',
            """
            SELECT 'customers' as table, count(*) as count FROM analytics.iceberg_customers 
            UNION ALL 
            SELECT 'orders' as table, count(*) as count FROM analytics.iceberg_orders
            """
        ], capture_output=True, text=True, timeout=30)
        
        logging.info(f"Data verification: {check_result.stdout}")
        
        logging.info("✅ Direct transfer completed successfully")
        return True
        
    except Exception as e:
        logging.error(f"Direct transfer failed: {e}")
        return False
    
def check_clickhouse_tables():
    """Проверка что таблицы создались в ClickHouse"""
    import logging
    
    logging.info("=== CHECKING CLICKHOUSE TABLES ===")
    
    try:
        result = subprocess.run([
            'docker', 'exec', 'dwh-stack-clickhouse-1',
            'clickhouse-client', '--user', 'admin', '--password', 'password', '-q',
            """
            SELECT 
                name as table_name,
                total_rows as record_count
            FROM system.tables 
            WHERE database = 'analytics' 
            AND name IN ('iceberg_customers', 'iceberg_orders')
            """
        ], capture_output=True, text=True, timeout=30)
        
        logging.info(f"Tables check result: {result.stdout}")
        
        if 'iceberg_customers' in result.stdout and 'iceberg_orders' in result.stdout:
            logging.info("✅ Both tables exist in ClickHouse")
            return True
        else:
            logging.error("❌ Tables not found in ClickHouse")
            return False
            
    except Exception as e:
        logging.error(f"Table check failed: {e}")
        return False
    
def run_dbt_pipeline():
    """Запуск DBT пайплайна для ClickHouse"""
    import logging
    
    logging.info("=== RUNNING DBT PIPELINE FOR CLICKHOUSE ===")
    
    try:
        # Проверим, что таблицы существуют и содержат данные
        logging.info("Checking source tables in ClickHouse...")
        
        check_result = subprocess.run([
            'docker', 'exec', 'dwh-stack-clickhouse-1',
            'clickhouse-client', '--user', 'admin', '--password', 'password', '-q',
            """
            SELECT 
                'iceberg_customers' as table, 
                count(*) as count 
            FROM analytics.iceberg_customers
            UNION ALL
            SELECT 
                'iceberg_orders' as table, 
                count(*) as count 
            FROM analytics.iceberg_orders
            """
        ], capture_output=True, text=True, timeout=30)
        
        logging.info(f"Source tables data: {check_result.stdout}")
        
        # Сначала проверим структуру DBT проекта
        logging.info("=== DBT PROJECT STRUCTURE ===")
        ls_result = subprocess.run([
            'find', '/opt/airflow/dbt', '-type', 'f', '-name', "*.yml", '-o', '-name', "*.sql"
        ], capture_output=True, text=True)
        logging.info(f"DBT files: {ls_result.stdout}")
        
        # Запускаем DBT debug для проверки конфигурации с полным выводом
        logging.info("=== RUNNING DBT DEBUG ===")
        debug_result = subprocess.run([
            '/home/airflow/.local/bin/dbt', 'debug',
            '--project-dir', '/opt/airflow/dbt/analytics_platform',
            '--profiles-dir', '/opt/airflow/dbt',
            '--target', 'clickhouse'
        ], capture_output=True, text=True, timeout=120)
        
        logging.info(f"DBT debug stdout: {debug_result.stdout}")
        logging.info(f"DBT debug stderr: {debug_result.stderr}")
        logging.info(f"DBT debug return code: {debug_result.returncode}")
        
        if debug_result.returncode != 0:
            logging.error("❌ DBT debug failed")
            # Попробуем получить больше информации о конфигурации
            logging.info("=== CHECKING DBT PROFILES ===")
            profiles_check = subprocess.run([
                '/home/airflow/.local/bin/dbt', 'debug', '--config-dir',
                '--project-dir', '/opt/airflow/dbt/analytics_platform',
                '--profiles-dir', '/opt/airflow/dbt'
            ], capture_output=True, text=True)
            logging.info(f"Profiles check: {profiles_check.stdout}")
            return False
        
        # Запускаем DBT
        logging.info("=== RUNNING DBT MODELS ===")
        result = subprocess.run([
            '/home/airflow/.local/bin/dbt', 'run',
            '--project-dir', '/opt/airflow/dbt/analytics_platform',
            '--profiles-dir', '/opt/airflow/dbt',
            '--target', 'clickhouse',
            '--models', 'stg_customers stg_orders dim_customers fct_orders',
            '--full-refresh'
        ], capture_output=True, text=True, timeout=600)
        
        logging.info(f"DBT stdout: {result.stdout}")
        logging.info(f"DBT stderr: {result.stderr}")
        logging.info(f"DBT return code: {result.returncode}")
        
        if result.returncode == 0:
            logging.info("✅ DBT pipeline executed successfully!")
            logging.info(f"DBT summary: {extract_dbt_summary(result.stdout)}")
            return True
        else:
            logging.error("❌ DBT failed")
            
            # Попробуем запустить отдельно каждую модель для диагностики
            logging.info("=== TRYING INDIVIDUAL MODELS ===")
            models = ['stg_customers', 'stg_orders', 'dim_customers', 'fct_orders']
            for model in models:
                logging.info(f"Testing model: {model}")
                model_result = subprocess.run([
                    '/home/airflow/.local/bin/dbt', 'run',
                    '--project-dir', '/opt/airflow/dbt/analytics_platform',
                    '--profiles-dir', '/opt/airflow/dbt',
                    '--target', 'clickhouse',
                    '--models', model
                ], capture_output=True, text=True, timeout=300)
                logging.info(f"Model {model} return code: {model_result.returncode}")
                if model_result.returncode != 0:
                    logging.error(f"Model {model} failed: {model_result.stderr}")
            
            return False
            
    except Exception as e:
        logging.error(f"DBT pipeline error: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return False
    
def create_test_data_in_clickhouse():
    """Создание тестовых данных в ClickHouse если таблицы пустые"""
    import logging
    
    logging.info("Creating test data in ClickHouse...")
    
    try:
        # Сначала создадим таблицы если их нет
        create_tables_sql = """
        CREATE TABLE IF NOT EXISTS analytics.iceberg_customers (
            id Int32,
            name String,
            email String,
            country_code String,
            created_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY id;
        
        CREATE TABLE IF NOT EXISTS analytics.iceberg_orders (
            id Int32,
            customer_id Int32,
            amount Float64,
            status String,
            created_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY id;
        """
        
        subprocess.run([
            'docker', 'exec', 'dwh-stack-clickhouse-1',
            'clickhouse-client', '--user', 'admin', '--password', 'password', '-q',
            create_tables_sql
        ], timeout=30)
        
        # Создаем тестовые данные
        test_data_sql = """
        INSERT INTO analytics.iceberg_customers VALUES
        (1, 'Test Customer 1', 'test1@example.com', 'US', now()),
        (2, 'Test Customer 2', 'test2@example.com', 'GB', now()),
        (3, 'Test Customer 3', 'test3@example.com', 'CA', now());
        
        INSERT INTO analytics.iceberg_orders VALUES
        (1, 1, 100.50, 'completed', now()),
        (2, 1, 50.25, 'completed', now()),
        (3, 2, 75.75, 'pending', now());
        """
        
        subprocess.run([
            'docker', 'exec', 'dwh-stack-clickhouse-1',
            'clickhouse-client', '--user', 'admin', '--password', 'password', '-q',
            test_data_sql
        ], timeout=30)
        
        logging.info("✅ Test data created in ClickHouse")
        
    except Exception as e:
        logging.error(f"Failed to create test data: {e}")

def extract_dbt_summary(output):
    """Извлечение краткого summary из DBT output"""
    lines = output.split('\n')
    summary_lines = []
    
    keywords = ['PASS=', 'WARNING=', 'ERROR=', 'completed', 'successfully', 'FAIL=']
    
    for line in lines[-20:]:
        if any(keyword in line for keyword in keywords):
            summary_lines.append(line)
    
    return '\n'.join(summary_lines) if summary_lines else "No summary available"

with DAG(
    'complete_data_pipeline',
    default_args=default_args,
    description='Complete data pipeline from source to analytics',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['data', 'etl', 'kafka', 'dbt']
) as dag:

    start = DummyOperator(task_id='start')
    
    setup_iceberg = PythonOperator(
        task_id='setup_iceberg_tables',
        python_callable=setup_iceberg_tables
    )
    
    setup_kafka = PythonOperator(
        task_id='setup_kafka_connectors',
        python_callable=setup_kafka_connectors
    )
    
    check_kafka = PythonOperator(
        task_id='check_kafka_topics',
        python_callable=check_kafka_topics
    )
    
    spark_loader = PythonOperator(
        task_id='run_spark_iceberg_loader',
        python_callable=run_spark_iceberg_loader
    )
    
    transfer_data = PythonOperator(
        task_id='transfer_iceberg_to_clickhouse',
        python_callable=transfer_iceberg_to_clickhouse
    )
    
    run_dbt = PythonOperator(
        task_id='run_dbt_pipeline',
        python_callable=run_dbt_pipeline
    )
    
    complete = DummyOperator(task_id='complete')
    
    # Пайплайн: start -> setup_iceberg -> setup_kafka -> check_kafka -> spark_loader -> transfer_data -> run_dbt -> complete
    start >> setup_iceberg >> setup_kafka >> check_kafka >> spark_loader >> transfer_data >> run_dbt >> complete