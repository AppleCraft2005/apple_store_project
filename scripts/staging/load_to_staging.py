import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
import glob
import sys # Tambahkan ini untuk exit code

# --- KONFIGURASI KONEKSI DATABASE ---
if os.path.exists('/.dockerenv'):
    DB_HOST = 'apple_store_postgres'
    LAKE_DIR = '/opt/airflow/datalake/raw'
else:
    DB_HOST = 'localhost'
    LAKE_DIR = './datalake/raw'

DB_USER = 'admin'
DB_PASS = 'admin'
DB_NAME = 'apple_dwh'
DB_PORT = '5432'

CONN_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def get_latest_file(pattern, search_dir=LAKE_DIR):
    """Mencari file terbaru berdasarkan pola nama"""
    full_pattern = os.path.join(search_dir, pattern)
    files = glob.glob(full_pattern)
    if not files:
        return None
    return max(files, key=os.path.getctime)

def load_csv_to_postgres(engine, file_path, table_name, columns_mapping=None, transform_func=None):
    try:
        print(f"--> Memproses: {os.path.basename(file_path)} ke tabel {table_name}")
        df = pd.read_csv(file_path)
        
        if transform_func:
            df = transform_func(df)
            
        if columns_mapping:
            df = df.rename(columns=columns_mapping)
            valid_cols = [c for c in df.columns if c in columns_mapping.values()]
            df = df[valid_cols]
        
        df.to_sql(table_name, engine, schema='staging', if_exists='append', index=False, method='multi')
        print(f"    [SUKSES] Terload {len(df)} baris.")
        
    except Exception as e:
        print(f"    [ERROR] Gagal load {table_name}: {e}")
        # Opsional: raise e jika ingin stop total jika satu file gagal

# --- FUNGSI TRANSFORMASI ---
def transform_employees(df):
    df['employee_name'] = df['First Name'] + ' ' + df['Last Name']
    df['jabatan'] = 'Apple Specialist'
    df['store_id'] = '1001' 
    return df

def transform_customers(df):
    df['Age'] = df['Age'].astype(str)
    return df

def load_json_specs(engine):
    try:
        specs_dir = os.path.join(LAKE_DIR, 'api_specs')
        json_file = get_latest_file('gsmarena_specs*.json', search_dir=specs_dir) # Sesuaikan pola nama file JSON Anda
        
        # Fallback coba cari pola lain jika gsmarena ga ketemu (misal techspecs)
        if not json_file:
             json_file = get_latest_file('techspecs*.json', search_dir=specs_dir)

        if not json_file:
            print(f"[SKIP] Tidak ada file JSON di {specs_dir}")
            return

        print(f"--> Memproses JSON: {os.path.basename(json_file)}")
        
        with open(json_file, 'r') as f:
            data = json.load(f)
            
        rows = []
        for item in data:
            prod_name = item.get('csv_product_name_reference') or item.get('csv_product_name') or 'Unknown'
            rows.append({
                'csv_product_name': prod_name,
                'raw_content': json.dumps(item) 
            })
            
        df_json = pd.DataFrame(rows)
        df_json.to_sql('stg_specs_raw', engine, schema='staging', if_exists='append', index=False)
        print(f"    [SUKSES] Terload {len(df_json)} dokumen JSON.")

    except Exception as e:
        print(f"    [ERROR] Gagal load JSON: {e}")
        raise e # Stop jika JSON gagal

# --- MAIN ---
def run_staging_load():
    print("=== MEMULAI LOAD KE STAGING ===")
    
    # 1. Buat Engine & Bersihkan Tabel
    try:
        engine = create_engine(CONN_STRING)
        
        # PERBAIKAN: Gunakan engine.begin() agar otomatis commit transaksi
        # Ini mengatasi error 'no attribute commit'
        with engine.begin() as conn:
            print("[OK] Koneksi Database Berhasil.")
            print("    Membersihkan tabel staging lama...")
            conn.execute(text("TRUNCATE TABLE staging.stg_sales, staging.stg_products, staging.stg_customers, staging.stg_employees, staging.stg_stores, staging.stg_category, staging.stg_warranty, staging.stg_specs_raw RESTART IDENTITY CASCADE;"))
            
    except Exception as e:
        print(f"[FATAL] Gagal koneksi/truncate database: {e}")
        sys.exit(1) # <--- PENTING: Paksa script Error agar Airflow jadi MERAH

    # 2. LOAD CSV
    try:
        file = get_latest_file('products*.csv'); load_csv_to_postgres(engine, file, 'stg_products', {'Product_ID': 'product_id', 'Product_Name': 'product_name', 'Price': 'price', 'Category_ID': 'category_id', 'gsmarena_key': 'gsmarena_key'}) if file else None
        
        file = get_latest_file('sales*.csv'); load_csv_to_postgres(engine, file, 'stg_sales', {'sale_id': 'sale_id', 'product_id': 'product_id', 'store_id': 'store_id', 'quantity': 'quantity', 'sale_date': 'sale_date'}) if file else None
        
        file = get_latest_file('stores*.csv'); load_csv_to_postgres(engine, file, 'stg_stores', {'Store_ID': 'store_id', 'Store_Name': 'store_name', 'City': 'city', 'Country': 'country'}) if file else None
        
        file = get_latest_file('category*.csv'); load_csv_to_postgres(engine, file, 'stg_category', {'category_id': 'category_id', 'category_name': 'category_name'}) if file else None
        
        file = get_latest_file('warranty*.csv'); load_csv_to_postgres(engine, file, 'stg_warranty', {'claim_id': 'claim_id', 'sale_id': 'sale_id', 'repair_status': 'repair_status', 'claim_date': 'claim_date'}) if file else None

        file = get_latest_file('employees*.csv')
        if file:
            mapping = {'Index': 'salesperson_id', 'employee_name': 'employee_name', 'jabatan': 'jabatan', 'store_id': 'store_id'}
            load_csv_to_postgres(engine, file, 'stg_employees', mapping, transform_func=transform_employees)

        file = get_latest_file('customers*.csv')
        if file:
            mapping = {'CustomerID': 'customer_id', 'FullName': 'customer_name', 'Age': 'age_group', 'CreditScore': 'membership_status'}
            load_csv_to_postgres(engine, file, 'stg_customers', mapping, transform_func=transform_customers)

        # 3. LOAD JSON
        load_json_specs(engine)
        
    except Exception as e:
        print(f"[FATAL] Gagal proses loading: {e}")
        sys.exit(1) # Stop Airflow jika ada error

    print("=== LOAD STAGING SELESAI ===")

if __name__ == "__main__":
    run_staging_load()