import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime

# --- KONFIGURASI ---
if os.path.exists('/.dockerenv'):
    DB_HOST = 'apple_store_postgres'
else:
    DB_HOST = 'localhost'

DB_USER = 'admin'
DB_PASS = 'admin'
DB_NAME = 'apple_dwh'
DB_PORT = '5432'
CONN_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def generate_churn_data():
    print("=== MEMULAI FEATURE ENGINEERING (CUSTOMER CHURN) ===")
    engine = create_engine(CONN_STRING)
    
    # 1. Query Agregasi (RFM)
    # Kita merangkum histori belanja setiap pelanggan
    query = """
    SELECT 
        c.customer_key,
        c.age_group,
        c.membership_status,
        -- Mengambil tanggal belanja terakhir (untuk Recency)
        MAX(d.full_date) as last_purchase_date,
        -- Menghitung total transaksi (Frequency)
        COUNT(s.sales_key) as frequency,
        -- Menghitung total uang yang dihabiskan (Monetary)
        SUM(s.total_amount) as monetary,
        -- Tambahan: Rata-rata belanja
        AVG(s.total_amount) as avg_spend
    FROM dwh.fact_sales s
    JOIN dwh.dim_customer c ON s.customer_key = c.customer_key
    JOIN dwh.dim_date d ON s.date_key = d.date_key
    GROUP BY c.customer_key, c.age_group, c.membership_status
    """
    
    print("--> Mengambil data dan melakukan agregasi per Pelanggan...")
    df = pd.read_sql(query, engine)

    
    
    # 2. Hitung Recency & Label Churn
    # Kita anggap 'Hari Ini' adalah tanggal terakhir yang ada di data penjualan (agar realistis)
    df['last_purchase_date'] = pd.to_datetime(df['last_purchase_date'])
    reference_date = df['last_purchase_date'].max()
    print(f"--> Reference Date (Hari Ini): {reference_date.date()}")
    
    # Hitung selisih hari (Recency)
    df['recency_days'] = (reference_date - df['last_purchase_date']).dt.days
    
    # --- TAMBAHAN DEBUGGING (Letakkan disini) ---
    print("\n--> Statistik Recency (Hari sejak belanja terakhir):")
    print(df['recency_days'].describe())
    # --------------------------------------------
    
    # DEFINISI CHURN:
    # Jika sudah tidak belanja > 90 hari (3 bulan), dianggap Churn (1)
    CHURN_THRESHOLD_DAYS = 7
    df['is_churn'] = df['recency_days'].apply(lambda x: 1 if x > CHURN_THRESHOLD_DAYS else 0)
    
    print(f"--> Data Pelanggan: {len(df)} orang.")
    print(f"--> Distribusi Churn:\n{df['is_churn'].value_counts()}")
    
    # 3. Simpan Hasil
    output_path = './scripts/ml/training_data_churn.csv'
    df.to_csv(output_path, index=False)
    print(f"--> Data latih disimpan di: {output_path}")

if __name__ == "__main__":
    generate_churn_data()