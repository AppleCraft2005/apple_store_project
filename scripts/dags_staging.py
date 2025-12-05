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
    '02_staging_pipeline',      # Nama yang muncul di Dashboard
    default_args=default_args, 
    schedule_interval=None,     # Trigger manual (atau ganti '@daily' nanti)
    catchup=False,
    tags=['apple_store', 'staging', 'uas']
) as dag:

    # --- TASK: LOAD TO STAGING ---
    # Menjalankan script python yang sudah Anda tes sukses tadi
    task_load_staging = BashOperator(
        task_id='run_load_to_staging',
        bash_command='python /opt/airflow/dags/scripts/staging/load_to_staging.py'
    )

    # Karena cuma 1 task, tidak perlu tanda panah (>>)