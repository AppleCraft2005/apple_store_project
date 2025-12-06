from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# --- KONFIGURASI ---
default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    '03_dwh_pipeline',          # Nama yang muncul di Dashboard
    default_args=default_args, 
    schedule_interval=None,     # Trigger manual
    catchup=False,
    tags=['apple_store', 'dwh', 'uas']
) as dag:

    # --- TASK: RUN ETL TRANSFORM ---
    # Menjalankan script python etl_transform.py yang ada di folder dwh
    task_transform_dwh = BashOperator(
        task_id='run_etl_transform_to_dwh',
        bash_command='python /opt/airflow/dags/scripts/dwh/etl_transform.py'
    )

    # Karena cuma 1 task utama, tidak perlu dependency