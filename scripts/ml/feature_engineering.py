import pandas as pd
from sqlalchemy import create_engine
import os

# --- KONFIGURASI KONEKSI DATABASE ---
if os.path.exists('/.dockerenv'):
    DB_HOST = 'apple_store_postgres'
else:
    DB_HOST = 'localhost'

DB_USER = 'admin'
DB_PASS = 'admin'
DB_NAME = 'apple_dwh'
DB_PORT = '5432'

CONN_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def generate_training_data():
    print("=== MEMULAI FEATURE ENGINEERING (WARRANTY RISK) ===")
    engine = create_engine(CONN_STRING)
    
    # 1. Query Gabungan (Sales + Warranty + Product Specs)
    query = """
    SELECT 
        s.sales_key,
        s.quantity,
        s.unit_price,
        
        -- Fitur Produk (Dari API)
        p.product_name,
        p.spec_chipset,
        p.spec_ram,
        p.spec_storage,
        p.spec_battery,
        cat.category_name,
        
        -- Target Label (1 = Klaim, 0 = Tidak)
        CASE WHEN w.warranty_key IS NOT NULL THEN 1 ELSE 0 END as is_claim
        
    FROM dwh.fact_sales s
    JOIN dwh.dim_product p ON s.product_key = p.product_key
    JOIN dwh.dim_category cat ON p.category_key = cat.category_key
    LEFT JOIN dwh.fact_warranty w ON s.sales_key = w.sales_key
    """
    
    print("--> Mengambil data dari DWH...")
    try:
        df = pd.read_sql(query, engine)
    except Exception as e:
        print(f"[FATAL] Gagal membaca database: {e}")
        return
    
    print(f"--> Data dimuat: {len(df)} baris.")
    print(f"--> Distribusi Label:\n{df['is_claim'].value_counts()}")
    
    # 2. Cleaning Ringan untuk ML
    # Isi nilai NULL dengan 'Unknown' agar tidak error saat training
    df.fillna('Unknown', inplace=True)
    
    # 3. Simpan Hasil (Dengan Deteksi Path Otomatis)
    if os.path.exists('/.dockerenv'):
        # Path Absolut di dalam Container Airflow
        base_dir = '/opt/airflow/dags/scripts/ml'
    else:
        # Path Relatif di Laptop
        base_dir = './scripts/ml'
        
    output_path = os.path.join(base_dir, 'training_data_warranty.csv')
    
    # Buat folder jika belum ada
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    df.to_csv(output_path, index=False)
    print(f"--> Data latih disimpan di: {output_path}")
    print("=== SELESAI ===")

if __name__ == "__main__":
    generate_training_data()