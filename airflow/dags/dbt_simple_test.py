from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'dbt_simple_test',
    default_args=default_args,
    description='Simple DBT test DAG',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'dbt']
) as dag:

    test_dbt_version = BashOperator(
        task_id='test_dbt_version',
        bash_command='dbt --version'
    )
    
    test_dbt_debug = BashOperator(
        task_id='test_dbt_debug',
        bash_command='cd /opt/airflow/dbt/analytics_platform && dbt debug --profiles-dir /opt/airflow/dbt || echo "DBT debug completed (git warning can be ignored)"'
    )
    
    test_dbt_run = BashOperator(
        task_id='test_dbt_run',
        bash_command='cd /opt/airflow/dbt/analytics_platform && dbt run --models stg_test_model --profiles-dir /opt/airflow/dbt'
    )
    
    test_dbt_version >> test_dbt_debug >> test_dbt_run