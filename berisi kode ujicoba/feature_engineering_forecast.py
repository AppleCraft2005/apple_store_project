import pandas as pd
from sqlalchemy import create_engine
import os

# --- KONFIGURASI KONEKSI (Sama seperti sebelumnya) ---
if os.path.exists('/.dockerenv'):
    DB_HOST = 'apple_store_postgres'
    LAKE_DIR = '/opt/airflow/datalake/raw' # Path Docker (jika perlu save ke sana)
    OUTPUT_DIR = '/opt/airflow/dags/scripts/ml'
else:
    DB_HOST = 'localhost'
    LAKE_DIR = './datalake/raw'
    OUTPUT_DIR = './scripts/ml'

DB_USER = 'admin'
DB_PASS = 'admin'
DB_NAME = 'apple_dwh'
DB_PORT = '5432'
CONN_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def generate_forecast_data():
    print("=== MEMULAI FEATURE ENGINEERING (SALES FORECAST) ===")
    engine = create_engine(CONN_STRING)

    # 1. Query Agregasi Harian
    # Kita butuh Total Qty per Tanggal
    query = """
    SELECT
        d.full_date as date,
        SUM(f.quantity) as total_qty
    FROM dwh.fact_sales f
    JOIN dwh.dim_date d ON f.date_key = d.date_key
    GROUP BY d.full_date
    ORDER BY d.full_date
    """

    print("--> Mengambil data harian dari DWH...")
    df = pd.read_sql(query, engine)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')

    # 2. Feature Engineering (Lag & Rolling)
    # Ini kuncinya: Buat fitur masa lalu untuk memprediksi masa depan
    print("--> Membuat fitur Time Series...")

    # Lag 1: Penjualan kemarin
    df['lag_1'] = df['total_qty'].shift(1)
    # Lag 7: Penjualan minggu lalu
    df['lag_7'] = df['total_qty'].shift(7)

    # Rolling Mean: Rata-rata 7 hari terakhir
    df['rolling_mean_7'] = df['total_qty'].rolling(window=7).mean()

    # Ekstrak Info Tanggal
    df['day_of_week'] = df.index.dayofweek
    df['day_of_month'] = df.index.day
    df['month'] = df.index.month
    df['is_weekend'] = df['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)

    # Hapus baris kosong (NaN) akibat shift/rolling di awal data
    df = df.dropna()

    print(f"--> Data siap latih: {len(df)} baris.")

    # 3. Simpan
    output_path = os.path.join(OUTPUT_DIR, 'training_data_forecast.csv')
    df.to_csv(output_path)
    print(f"--> Disimpan di: {output_path}")

if __name__ == "__main__":
    generate_forecast_data()