from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    '00_init_db_pipeline',  # DAG Nomor 0 (Paling Awal)
    default_args=default_args, 
    schedule_interval=None, # Hanya jalan manual (sekali saja)
    catchup=False,
    tags=['apple_store', 'setup', 'uas']
) as dag:

    task_init_db = BashOperator(
        task_id='initialize_database_tables',
        bash_command='python /opt/airflow/dags/scripts/sql/init_db.py'
    )