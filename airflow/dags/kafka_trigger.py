from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def check_kafka_for_new_data():
    """Проверяем наличие новых сообщений в Kafka топиках Debezium"""
    import subprocess
    import json
    import logging
    
    try:
        # Проверяем правильный топик
        result = subprocess.run([
            'docker', 'exec', 'dwh-stack-kafka-1',
            'kafka-console-consumer',
            '--bootstrap-server', 'localhost:9092',
            '--topic', 'postgres-server.public.customers', 
            '--max-messages', '1',
            '--timeout-ms', '10000',  # Увеличиваем таймаут
            '--from-beginning'
        ], capture_output=True, text=True, timeout=15)
        
        logging.info(f"Kafka check result: {result.returncode}")
        logging.info(f"Kafka stdout: {result.stdout}")
        logging.info(f"Kafka stderr: {result.stderr}")
        
        # Если есть вывод - есть новые данные
        if result.stdout and len(result.stdout.strip()) > 0:
            logging.info("✅ New data detected in Kafka")
            return True
        else:
            logging.info("⏳ No new data in Kafka")
            return False
            
    except Exception as e:
        logging.error(f"Kafka check issue: {e}")
        return False
    
with DAG(
    'auto_trigger_pipeline',
    default_args=default_args,
    description='Automatically trigger pipeline on new PostgreSQL data',
    schedule_interval=timedelta(minutes=5),  # Проверяем каждые 5 минут
    catchup=False,
    tags=['auto', 'cdc', 'trigger']
) as dag:
    
    # Сенсор который проверяет Kafka на новые данные
    check_new_data = PythonSensor(
        task_id='check_for_new_data',
        python_callable=check_kafka_for_new_data,
        mode='reschedule',
        timeout=300,  # 5 минут
        poke_interval=60  # Проверяем каждую минуту
    )
    
    # Запускаем основной пайплайн
    trigger_pipeline = TriggerDagRunOperator(
        task_id='trigger_data_pipeline',
        trigger_dag_id='complete_data_pipeline',
        wait_for_completion=False,
        conf={'trigger_source': 'auto_cdc'}
    )
    
    check_new_data >> trigger_pipeline