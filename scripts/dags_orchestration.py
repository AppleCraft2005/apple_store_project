from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# --- KONFIGURASI DASAR ---
default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 1, 1),
    'retries': 0, # Kita set 0 dulu biar kalau gagal langsung ketahuan (merah)
}

# --- DEFINISI DAG ---
with DAG(
    '01_ingestion_pipeline',  # ID yang muncul di Dashboard Airflow
    default_args=default_args, 
    schedule_interval=None,   # None = Jalan hanya kalau dipencet (Trigger)
    catchup=False,
    tags=['apple_store', 'ingestion', 'uas']
) as dag:

    # --- TASK 1: INGEST CSV (Internal Data) ---
    # Perintah ini menjalankan script Python di dalam container Docker
    task_ingest_csv = BashOperator(
        task_id='ingest_csv_to_datalake',
        bash_command='python /opt/airflow/dags/scripts/ingestion/ingest_csv.py'
    )

    # --- TASK 2: INGEST API (External Data) ---
    # Perintah ini menjalankan script Python penarik API
    task_ingest_api = BashOperator(
        task_id='ingest_api_to_datalake',
        bash_command='python /opt/airflow/dags/scripts/ingestion/ingest_api.py'
    )

    # --- ALUR EKSEKUSI (DEPENDENCY) ---
    # Task CSV harus sukses dulu, baru Task API jalan
    task_ingest_csv >> task_ingest_api