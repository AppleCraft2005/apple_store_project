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
    '04_ml_pipeline',            # Nama yang muncul di Dashboard
    default_args=default_args, 
    schedule_interval=None,      # Trigger manual (atau @monthly)
    catchup=False,
    tags=['apple_store', 'machine_learning', 'uas']
) as dag:

    # RANDOM FOREST
    # --- TASK 1: FEATURE ENGINEERING ---
    # Mengambil data dari DWH -> Membuat training_data_warranty.csv
    task_feature_eng = BashOperator(
        task_id='feature_engineering_warranty',
        bash_command='python /opt/airflow/dags/scripts/ml/feature_engineering.py'
    )

    # --- TASK 2: MODEL TRAINING (RANDOM FOREST) ---
    # Membaca CSV -> Melatih Model -> Simpan .pkl
    # Note: Kita arahkan ke 'train_model.py' (Random Forest), bukan XGBoost
    task_train_model = BashOperator(
        task_id='train_random_forest_model',
        bash_command='python /opt/airflow/dags/scripts/ml/train_model.py'
    )

    # # XGBOOST
    # # --- TASK 1: FEATURE ENGINEERING ---
    # # Mengambil data dari DWH, membuat fitur, simpan ke CSV
    # task_feature_eng = BashOperator(
    #     task_id='feature_engineering',
    #     bash_command='python /opt/airflow/dags/scripts/ml/feature_engineering.py'
    # )

    # # --- TASK 2: MODEL TRAINING ---
    # # Membaca CSV, melatih XGBoost, simpan Model .pkl
    # task_train_model = BashOperator(
    #     task_id='train_xgboost_model',
    #     bash_command='python /opt/airflow/dags/scripts/ml/train_model_xgboost.py'
    # )   

    # --- ALUR ---
    # Data harus siap dulu (Feature Eng), baru bisa Training
    task_feature_eng >> task_train_model