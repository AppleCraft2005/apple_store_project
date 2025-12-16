# import pandas as pd
# from sqlalchemy import create_engine
# import os

# # --- KONFIGURASI KONEKSI DATABASE ---
# if os.path.exists('/.dockerenv'):
#     DB_HOST = 'apple_store_postgres'
# else:
#     DB_HOST = 'localhost'

# DB_USER = 'admin'
# DB_PASS = 'admin'
# DB_NAME = 'apple_dwh'
# DB_PORT = '5432'

# CONN_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# def generate_training_data():
#     print("=== MEMULAI FEATURE ENGINEERING (WARRANTY RISK) ===")
#     engine = create_engine(CONN_STRING)
    
#     # 1. Query Gabungan (Sales + Warranty + Product Specs)
#     query = """
#     SELECT 
#         s.sales_key,
#         s.quantity,
#         s.unit_price,
        
#         -- Fitur Produk (Dari API)
#         p.product_name,
#         p.spec_chipset,
#         p.spec_ram,
#         p.spec_storage,
#         p.spec_battery,
#         cat.category_name,
        
#         -- Target Label (1 = Klaim, 0 = Tidak)
#         CASE WHEN w.warranty_key IS NOT NULL THEN 1 ELSE 0 END as is_claim
        
#     FROM dwh.fact_sales s
#     JOIN dwh.dim_product p ON s.product_key = p.product_key
#     JOIN dwh.dim_category cat ON p.category_key = cat.category_key
#     LEFT JOIN dwh.fact_warranty w ON s.sales_key = w.sales_key
#     """
    
#     print("--> Mengambil data dari DWH...")
#     try:
#         df = pd.read_sql(query, engine)
#     except Exception as e:
#         print(f"[FATAL] Gagal membaca database: {e}")
#         return
    
#     print(f"--> Data dimuat: {len(df)} baris.")
#     print(f"--> Distribusi Label:\n{df['is_claim'].value_counts()}")
    
#     # 2. Cleaning Ringan untuk ML
#     # Isi nilai NULL dengan 'Unknown' agar tidak error saat training
#     df.fillna('Unknown', inplace=True)
    
#     # 3. Simpan Hasil (Dengan Deteksi Path Otomatis)
#     if os.path.exists('/.dockerenv'):
#         # Path Absolut di dalam Container Airflow
#         base_dir = '/opt/airflow/dags/scripts/ml'
#     else:
#         # Path Relatif di Laptop
#         base_dir = './scripts/ml'
        
#     output_path = os.path.join(base_dir, 'training_data_warranty.csv')
    
#     # Buat folder jika belum ada
#     os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
#     df.to_csv(output_path, index=False)
#     print(f"--> Data latih disimpan di: {output_path}")
#     print("=== SELESAI ===")

# if __name__ == "__main__":
#     generate_training_data()

# import pandas as pd
# from sqlalchemy import create_engine
# import os
# import random

# # --- KONFIGURASI KONEKSI ---
# if os.path.exists('/.dockerenv'):
#     DB_HOST = 'apple_store_postgres'
#     OUTPUT_DIR = '/opt/airflow/dags/scripts/ml'
# else:
#     DB_HOST = 'localhost'
#     OUTPUT_DIR = './scripts/ml'

# DB_USER = 'admin'; DB_PASS = 'admin'; DB_NAME = 'apple_dwh'; DB_PORT = '5432'
# CONN_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# def generate_balanced_training_data():
#     print("=== FEATURE ENGINEERING: BALANCED DATASET (UNDERSAMPLING) ===")
#     engine = create_engine(CONN_STRING)
    
#     # 1. Query Data Lengkap (Ambil semuanya dulu)
#     query = """
#     SELECT 
#         s.sales_key,
#         -- Fitur Produk
#         p.product_name, p.spec_chipset, p.spec_ram, p.spec_storage, p.spec_battery,
#         cat.category_name,
#         -- Target Label
#         CASE WHEN w.warranty_key IS NOT NULL THEN 1 ELSE 0 END as is_claim
#     FROM dwh.fact_sales s
#     JOIN dwh.dim_product p ON s.product_key = p.product_key
#     JOIN dwh.dim_category cat ON p.category_key = cat.category_key -- <--- PERBAIKAN DI SINI (c jadi cat)
#     LEFT JOIN dwh.fact_warranty w ON s.sales_key = w.sales_key
#     """
#     print("--> Mengambil data dari DWH...")
#     df = pd.read_sql(query, engine)
    
#     # 2. Pisahkan Data (Klaim vs Aman)
#     df_claim = df[df['is_claim'] == 1]
#     df_safe = df[df['is_claim'] == 0]
    
#     print(f"--> Total Data Asli: {len(df)}")
#     print(f"    - Jumlah Klaim (1): {len(df_claim)}")
#     print(f"    - Jumlah Aman  (0): {len(df_safe)}")
    
#     # 3. Lakukan Undersampling (Pengurangan)
#     # Kita akan membuat rasio 1:1 (30.000 Klaim vs 30.000 Aman)
    
#     TARGET_SIZE_PER_CLASS = len(df_claim) # Jumlah Klaim (~30.000)
    
#     # Ambil sampel acak dari kelas Aman (Non-Klaim)
#     df_safe_sampled = df_safe.sample(n=TARGET_SIZE_PER_CLASS, random_state=42)
    
#     # Data Klaim (df_claim) diambil SEMUA
#     df_balanced = pd.concat([df_claim, df_safe_sampled])
    
#     # ... (lanjut ke langkah shuffle, cleaning, dan simpan) ...
    
#     # Acak urutannya agar tidak berkumpul (Shuffle)
#     df_balanced = df_balanced.sample(frac=1, random_state=42).reset_index(drop=True)
    
#     print(f"--> Data setelah reduce: {len(df_balanced)} baris.")
#     print(f"--> Distribusi Baru:\n{df_balanced['is_claim'].value_counts()}")
    
#     # 5. Cleaning & Simpan
#     df_balanced.fillna('Unknown', inplace=True)
    
#     output_path = os.path.join(OUTPUT_DIR, 'training_data_warranty.csv')
#     df_balanced.to_csv(output_path, index=False)
#     print(f"--> Data Balanced tersimpan di: {output_path}")

# if __name__ == "__main__":
#     generate_balanced_training_data()

import pandas as pd
from sqlalchemy import create_engine
import os

# --- KONFIGURASI KONEKSI ---
if os.path.exists('/.dockerenv'):
    DB_HOST = 'apple_store_postgres'
    base_dir = '/opt/airflow/dags/scripts/ml'
else:
    DB_HOST = 'localhost'
    base_dir = './scripts/ml'

DB_USER = 'admin'; DB_PASS = 'admin'; DB_NAME = 'apple_dwh'; DB_PORT = '5432'
CONN_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def generate_balanced_training_data():
    print("=== FEATURE ENGINEERING: INTERNAL DATA ONLY (NO API) ===")
    engine = create_engine(CONN_STRING)
    
    # 1. Query Data (HANYA DATA INTERNAL)
    # PERBAIKAN: Ganti 'c.category_key' menjadi 'cat.category_key'
    query = """
    SELECT 
        s.sales_key,
        -- Fitur Internal
        p.product_name,
        p.price,              -- Harga (Fitur baru pengganti spek)
        cat.category_name,    -- Kategori (Fitur utama sekarang)
        
        -- Target Label
        CASE WHEN w.warranty_key IS NOT NULL THEN 1 ELSE 0 END as is_claim
    FROM dwh.fact_sales s
    JOIN dwh.dim_product p ON s.product_key = p.product_key
    JOIN dwh.dim_category cat ON p.category_key = cat.category_key -- <--- SUDAH DIPERBAIKI (c -> cat)
    LEFT JOIN dwh.fact_warranty w ON s.sales_key = w.sales_key
    """
    
    print("--> Mengambil data dari DWH...")
    try:
        df = pd.read_sql(query, engine)
    except Exception as e:
        print(f"[FATAL] Gagal query: {e}")
        return
    
    # 2. Pisahkan Data (Klaim vs Aman)
    df_claim = df[df['is_claim'] == 1]
    df_safe = df[df['is_claim'] == 0]
    
    print(f"--> Total Data: {len(df)} (Klaim: {len(df_claim)} vs Aman: {len(df_safe)})")
    
    # 3. Undersampling (Seimbangkan Data)
    TARGET_SIZE = len(df_claim) # Sekitar 30rb
    
    # Ambil sampel dari data aman
    df_safe_sampled = df_safe.sample(n=TARGET_SIZE, random_state=42)
    df_balanced = pd.concat([df_claim, df_safe_sampled])
    
    # Shuffle (Acak urutan)
    df_balanced = df_balanced.sample(frac=1, random_state=42).reset_index(drop=True)
    
    print(f"--> Data Balanced Final: {len(df_balanced)} baris.")
    
    # 4. Simpan
    output_path = os.path.join(base_dir, 'training_data_warranty.csv')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    df_balanced.to_csv(output_path, index=False)
    print(f"--> File tersimpan: {output_path}")

if __name__ == "__main__":
    generate_balanced_training_data()