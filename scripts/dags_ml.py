from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# --- KONFIGURASI ---
default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 1, 1),
    'retries': 0, # Tidak perlu retry otomatis untuk ML training
}

with DAG(
    '04_ml_pipeline',            # Nama yang muncul di Dashboard Airflow
    default_args=default_args, 
    schedule_interval=None,      # Trigger Manual (atau ganti '0 3 * * 0' untuk Mingguan)
    catchup=False,
    tags=['apple_store', 'forecasting', 'uas']
) as dag:

    # --- TASK 1: FEATURE ENGINEERING (TIME SERIES) ---
    # Mengambil data dari DWH -> Pivot per Kategori -> Simpan CSV
    task_prep_data = BashOperator(
        task_id='prepare_forecast_data',
        bash_command='python /opt/airflow/dags/scripts/ml/feature_engineering_cat_forecast.py'
    )

    # --- TASK 2: MODEL TRAINING (XGBOOST) ---
    # Melatih model per kategori -> Hitung MAE/MAPE -> Simpan Metrics JSON
    task_train_model = BashOperator(
        task_id='train_forecast_model',
        bash_command='python /opt/airflow/dags/scripts/ml/train_cat_forecast.py'
    )

    # --- TASK 3: FUTURE PREDICTION (INFERENCE) ---
    # Menggunakan model untuk meramal 180 hari ke depan -> Simpan CSV untuk Dashboard
    task_predict_future = BashOperator(
        task_id='predict_future_sales',
        bash_command='python /opt/airflow/dags/scripts/ml/predict_future_sales.py'
    )

    # --- ALUR EKSEKUSI ---
    # Siapkan Data -> Latih Model -> Ramal Masa Depan
    task_prep_data >> task_train_model >> task_predict_future