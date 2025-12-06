import pandas as pd
from sqlalchemy import create_engine, text
import os
import json
import random

# --- KONFIGURASI KONEKSI ---
if os.path.exists('/.dockerenv'):
    DB_HOST = 'apple_store_postgres'
else:
    DB_HOST = 'localhost'

DB_USER = 'admin'
DB_PASS = 'admin'
DB_NAME = 'apple_dwh'
DB_PORT = '5432'

CONN_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def get_engine():
    return create_engine(CONN_STRING)

# ==============================================================================
# 1. FUNGSI HELPER: EXTRACT JSON DARI STAGING
# ==============================================================================
def process_dim_product(engine):
    print("--> Memproses Dim_Product (Join dengan JSON Specs)...")
    
    # Ambil data produk dasar dan data JSON spec dari staging
    query = """
    SELECT 
        p.product_id, 
        p.product_name, 
        p.price, 
        p.gsmarena_key,
        p.category_id,
        s.raw_content
    FROM staging.stg_products p
    LEFT JOIN staging.stg_specs_raw s ON p.product_name = s.csv_product_name
    """
    df = pd.read_sql(query, engine)
    
    # List untuk menyimpan hasil ekstraksi JSON
    specs_extracted = []

    for _, row in df.iterrows():
        raw_json = row['raw_content']
        spec_data = {
            'spec_chipset': None,
            'spec_ram': None,
            'spec_storage': None,
            'spec_display': None,
            'spec_camera': None,
            'spec_battery': None
        }
        
        # Logika Parsing JSON (Sesuaikan dengan struktur GSMArena Anda)
        if raw_json:
            # Jika formatnya dictionary (dari JSONB Postgres)
            if isinstance(raw_json, str):
                data = json.loads(raw_json)
            else:
                data = raw_json
            
            # Navigasi masuk ke dalam JSON (sesuai struktur yang Anda share sebelumnya)
            # Struktur: data -> specs -> ...
            # Atau jika struktur flat langsung di root
            try:
                specs = data.get('specs', {}) 
                if not specs: specs = data

                spec_data['spec_chipset'] = specs.get('chipset')
                
                # --- CLEANING RAM ---
                raw_ram = specs.get('ram')
                if raw_ram:
                    # Hapus kata "RAM" dan spasi di sekitarnya
                    spec_data['spec_ram'] = str(raw_ram).replace('RAM', '').strip()
                
                # --- CLEANING STORAGE ---
                raw_storage = specs.get('storage_options') or specs.get('storage')
                if raw_storage:
                    # Hapus kata-kata yang tidak perlu
                    clean_storage = str(raw_storage).replace('storage', '').replace(', no card slot', '').strip()
                    # Opsional: Hapus spasi berlebih
                    spec_data['spec_storage'] = clean_storage
                
                spec_data['spec_display'] = specs.get('display_size')
                spec_data['spec_camera'] = specs.get('camera')
                spec_data['spec_battery'] = specs.get('battery')
            except Exception as e:
                # print(f"Error parsing JSON for {row['product_name']}: {e}")
                pass

        specs_extracted.append(spec_data)
    
    # Gabungkan hasil ekstraksi kembali ke DataFrame utama
    df_specs = pd.DataFrame(specs_extracted)
    df_final = pd.concat([df, df_specs], axis=1)
    
    # Mapping ke Tabel DWH
    df_load = pd.DataFrame({
        'product_id_original': df_final['product_id'],
        'product_name': df_final['product_name'],
        'price': df_final['price'],
        'spec_chipset': df_final['spec_chipset'],
        'spec_ram': df_final['spec_ram'],
        'spec_storage': df_final['spec_storage'],
        'spec_display': df_final['spec_display'],
        'spec_camera': df_final['spec_camera'],
        'spec_battery': df_final['spec_battery'],
        # Nanti kita update category_key setelah Dim_Category terisi
        'category_id_temp': df_final['category_id'] 
    })
    
    # Load ke DWH (Tanpa Foreign Key dulu, nanti di-update atau pakai lookup)
    # Strategi simple: Load category dulu, baru lookup.
    # Disini kita simpan dulu ke DWH
    return df_load

# ==============================================================================
# 2. FUNGSI LOAD DIMENSI SEDERHANA
# ==============================================================================
def load_simple_dimensions(engine):
    print("--> Memproses Dimensi Sederhana (Store, Category, Customer, Employee)...")
    
    # 1. Dim Category
    df_cat = pd.read_sql("SELECT category_id, category_name FROM staging.stg_category", engine)
    df_cat.rename(columns={'category_id': 'category_id_original'}, inplace=True)
    df_cat.to_sql('dim_category', engine, schema='dwh', if_exists='append', index=False)
    print("    Dim_Category [OK]")

    # 2. Dim Store
    df_store = pd.read_sql("SELECT store_id, store_name, city, country FROM staging.stg_stores", engine)
    df_store.rename(columns={'store_id': 'store_id_original'}, inplace=True)
    df_store.to_sql('dim_store', engine, schema='dwh', if_exists='append', index=False)
    print("    Dim_Store [OK]")

    # 3. Dim Customer
    df_cust = pd.read_sql("SELECT customer_id, customer_name, age_group, membership_status FROM staging.stg_customers", engine)
    df_cust.rename(columns={'customer_id': 'customer_id_original'}, inplace=True)
    df_cust.to_sql('dim_customer', engine, schema='dwh', if_exists='append', index=False)
    print("    Dim_Customer [OK]")

    # 4. Dim Employee (Butuh Store Key)
    # Kita baca employee staging dan lookup store_key dari dwh.dim_store
    df_emp = pd.read_sql("SELECT salesperson_id, employee_name, jabatan, store_id FROM staging.stg_employees", engine)
    
    # Ambil data Dim_Store yang baru saja diisi
    df_dim_store = pd.read_sql("SELECT store_key, store_id_original FROM dwh.dim_store", engine)
    
    # Join untuk dapatkan Surrogate Key (store_key)
    # Pastikan tipe data sama (string/object)
    df_emp['store_id'] = df_emp['store_id'].astype(str)
    df_dim_store['store_id_original'] = df_dim_store['store_id_original'].astype(str)
    
    df_merged = pd.merge(df_emp, df_dim_store, left_on='store_id', right_on='store_id_original', how='left')
    
    # Siapkan dataframe final
    df_emp_final = pd.DataFrame({
        'salesperson_id_original': df_merged['salesperson_id'],
        'employee_name': df_merged['employee_name'],
        'jabatan': df_merged['jabatan'],
        'store_key': df_merged['store_key'] # Ini Foreign Key
    })
    df_emp_final.to_sql('dim_employee', engine, schema='dwh', if_exists='append', index=False)
    print("    Dim_Employee [OK]")

def load_dim_product_final(engine, df_product_data):
    # Lookup Category Key
    print("--> Finalisasi Dim_Product...")
    df_dim_cat = pd.read_sql("SELECT category_key, category_id_original FROM dwh.dim_category", engine)
    
    # Join
    df_product_data['category_id_temp'] = df_product_data['category_id_temp'].astype(str)
    df_dim_cat['category_id_original'] = df_dim_cat['category_id_original'].astype(str)
    
    df_merged = pd.merge(df_product_data, df_dim_cat, left_on='category_id_temp', right_on='category_id_original', how='left')
    
    # Bersihkan kolom temp
    df_final = df_merged.drop(columns=['category_id_temp', 'category_id_original'])
    
    df_final.to_sql('dim_product', engine, schema='dwh', if_exists='append', index=False)
    print("    Dim_Product [OK]")

# ==============================================================================
# 3. FUNGSI DIMENSI TANGGAL (Update: Rename to dim_date & Fix Format)
# ==============================================================================
def process_dim_date(engine):
    print("--> Memproses Dim_Date...")
    
    # 1. Ambil data mentah
    dates_sales = pd.read_sql("SELECT DISTINCT sale_date FROM staging.stg_sales WHERE sale_date IS NOT NULL", engine)
    dates_warranty = pd.read_sql("SELECT DISTINCT claim_date FROM staging.stg_warranty WHERE claim_date IS NOT NULL", engine)
    
    dates_sales.rename(columns={'sale_date': 'tanggal_mentah'}, inplace=True)
    dates_warranty.rename(columns={'claim_date': 'tanggal_mentah'}, inplace=True)
    
    # 2. Gabungkan SEMUA data (jangan unique dulu di sini)
    all_dates = pd.concat([dates_sales, dates_warranty])
    df_dates = pd.DataFrame(all_dates, columns=['tanggal_mentah'])
    
    # 3. KONVERSI TANGGAL (Standardisasi)
    # Lakukan ini DULUAN agar '18-01-2024' dan '2024-01-18' jadi sama
    df_dates['full_date'] = pd.to_datetime(df_dates['tanggal_mentah'], format='mixed', dayfirst=True)
    
    # 4. HAPUS DUPLIKAT (SETELAH KONVERSI)
    # Ini langkah krusial yang kurang tadi
    df_dates = df_dates.drop_duplicates(subset=['full_date'])
    
    # 5. Buat kolom turunan
    df_dates['date_key'] = df_dates['full_date'].dt.strftime('%Y%m%d').astype(int)
    df_dates['day_name'] = df_dates['full_date'].dt.day_name()
    df_dates['month_name'] = df_dates['full_date'].dt.month_name()
    df_dates['year'] = df_dates['full_date'].dt.year
    df_dates['quarter'] = df_dates['full_date'].dt.quarter
    df_dates['is_weekend'] = df_dates['full_date'].dt.dayofweek >= 5
    
    # Buang kolom sampah
    df_final = df_dates.drop(columns=['tanggal_mentah'])
    
    # 6. Load ke DB
    df_final.to_sql('dim_date', engine, schema='dwh', if_exists='append', index=False)
    print(f"    Dim_Date [OK] - {len(df_final)} hari unik.")

# ==============================================================================
# 4. FUNGSI LOAD FACT SALES (Perbaikan Tipe Data & Format Tanggal)
# ==============================================================================
def process_fact_sales(engine):
    print("--> Memproses Fact_Sales (Simulasi Relasi)...")
    
    # Baca Data Staging
    df_sales = pd.read_sql("SELECT * FROM staging.stg_sales", engine)
    
    # Baca Lookup Keys dari DWH
    df_prod = pd.read_sql("SELECT product_key, product_id_original FROM dwh.dim_product", engine)
    df_store = pd.read_sql("SELECT store_key, store_id_original FROM dwh.dim_store", engine)
    
    # Ambil LIST ID untuk simulasi random
    cust_keys = pd.read_sql("SELECT customer_key FROM dwh.dim_customer", engine)['customer_key'].tolist()
    emp_keys = pd.read_sql("SELECT employee_key FROM dwh.dim_employee", engine)['employee_key'].tolist()
    
    # --- 1. Lookup Product Key ---
    df_sales['product_id'] = df_sales['product_id'].astype(str)
    df_prod['product_id_original'] = df_prod['product_id_original'].astype(str)
    df_sales = pd.merge(df_sales, df_prod, left_on='product_id', right_on='product_id_original', how='left')
    
    # --- 2. Lookup Store Key ---
    df_sales['store_id'] = df_sales['store_id'].astype(str)
    df_store['store_id_original'] = df_store['store_id_original'].astype(str)
    df_sales = pd.merge(df_sales, df_store, left_on='store_id', right_on='store_id_original', how='left')
    
    # --- 3. Create Date Key (Fix Warning Format) ---
    # Gunakan mixed format + dayfirst=True seperti di Dim_Date
    df_sales['sale_date'] = pd.to_datetime(df_sales['sale_date'], format='mixed', dayfirst=True)
    df_sales['date_key'] = df_sales['sale_date'].dt.strftime('%Y%m%d').fillna(0).astype(int)
    
    # --- 4. SIMULASI: Assign Random Customer & Employee ---
    print("    Melakukan simulasi Customer & Employee...")
    # Handle jika tabel customer/employee kosong (agar tidak error random.choice dari list kosong)
    if cust_keys and emp_keys:
        df_sales['customer_key'] = [random.choice(cust_keys) for _ in range(len(df_sales))]
        df_sales['employee_key'] = [random.choice(emp_keys) for _ in range(len(df_sales))]
    else:
        print("    [WARNING] Dim_Customer atau Dim_Employee kosong! Mengisi dengan NULL.")
        df_sales['customer_key'] = None
        df_sales['employee_key'] = None
    
    # --- 5. Hitung Total Amount (Fix TypeError) ---
    # Ambil harga dari Dim_Product
    df_prices = pd.read_sql("SELECT product_key, price FROM dwh.dim_product", engine)
    df_sales = pd.merge(df_sales, df_prices, on='product_key', how='left')
    
    # --- PERBAIKAN UTAMA: Konversi Tipe Data ---
    print("    Menghitung Total Amount...")
    # Bersihkan non-numeric characters jika ada (misal '$' atau ',')
    # Tapi karena di staging sudah NUMERIC/TEXT, pd.to_numeric adalah cara paling aman
    df_sales['quantity'] = pd.to_numeric(df_sales['quantity'], errors='coerce').fillna(0)
    df_sales['price'] = pd.to_numeric(df_sales['price'], errors='coerce').fillna(0)
    
    df_sales['unit_price'] = df_sales['price']
    df_sales['total_amount'] = df_sales['quantity'] * df_sales['unit_price']
    
    # --- 6. Final Selection ---
    df_fact = pd.DataFrame({
        'sales_id_original': df_sales['sale_id'],
        'date_key': df_sales['date_key'],
        'product_key': df_sales['product_key'],
        'store_key': df_sales['store_key'],
        'customer_key': df_sales['customer_key'],
        'employee_key': df_sales['employee_key'],
        'quantity': df_sales['quantity'],
        'unit_price': df_sales['unit_price'],
        'total_amount': df_sales['total_amount']
    })
    
    # Hapus baris yang key-nya NaN (Data kotor tidak bisa masuk DWH karena FK Constraint)
    df_fact = df_fact.dropna(subset=['date_key', 'product_key', 'store_key'])
    
    df_fact.to_sql('fact_sales', engine, schema='dwh', if_exists='append', index=False)
    print(f"    Fact_Sales [OK] - {len(df_fact)} transaksi berhasil dimuat.")

# ==============================================================================
# 5. FUNGSI LOAD FACT WARRANTY
# ==============================================================================
def process_fact_warranty(engine):
    print("--> Memproses Fact_Warranty...")
    
    # 1. Baca Data Staging
    df_warranty = pd.read_sql("SELECT * FROM staging.stg_warranty", engine)
    
    # 2. Lookup Sales Key (Hubungkan ke Fakta Penjualan)
    # Kita butuh tahu sales_key (PK di DWH) dari sale_id (ID asli di CSV)
    df_sales_key = pd.read_sql("SELECT sales_key, sales_id_original FROM dwh.fact_sales", engine)
    
    # Join
    df_warranty = pd.merge(df_warranty, df_sales_key, left_on='sale_id', right_on='sales_id_original', how='left')
    
    # 3. Create Date Key
    df_warranty['claim_date'] = pd.to_datetime(df_warranty['claim_date'], format='mixed', dayfirst=True)
    df_warranty['date_key'] = df_warranty['claim_date'].dt.strftime('%Y%m%d').fillna(0).astype(int)
    
    # 4. Validasi & Cleanup
    # Hapus baris jika sales_key-nya tidak ketemu (Data garansi yatim piatu)
    initial_count = len(df_warranty)
    df_warranty = df_warranty.dropna(subset=['sales_key'])
    final_count = len(df_warranty)
    
    if initial_count > final_count:
        print(f"    [INFO] {initial_count - final_count} data garansi dibuang karena ID Penjualan tidak ditemukan di Fact_Sales.")
    
    # 5. Final Selection
    df_fact = pd.DataFrame({
        'claim_id_original': df_warranty['claim_id'],
        'date_key': df_warranty['date_key'],
        'sales_key': df_warranty['sales_key'],
        'repair_status': df_warranty['repair_status']
    })
    
    df_fact.to_sql('fact_warranty', engine, schema='dwh', if_exists='append', index=False)
    print(f"    Fact_Warranty [OK] - {len(df_fact)} klaim berhasil dimuat.")

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================
def run_etl():
    print("=== MEMULAI TRANSFORMASI KE DWH ===")
    engine = get_engine()
    
    # ... (bagian cleaning & load dimensi lainnya biarkan sama) ...
    
    with engine.begin() as conn:
        print("    Cleaning DWH tables...")
        conn.execute(text("TRUNCATE TABLE dwh.fact_warranty, dwh.fact_sales, dwh.dim_employee, dwh.dim_product, dwh.dim_customer, dwh.dim_store, dwh.dim_category, dwh.dim_date RESTART IDENTITY CASCADE;"))
    
    load_simple_dimensions(engine)
    process_dim_date(engine)
    
    df_prod_prepared = process_dim_product(engine)
    load_dim_product_final(engine, df_prod_prepared)
    
    # 3. Load Fakta
    process_fact_sales(engine)
    
    # --- TAMBAHKAN BARIS INI ---
    process_fact_warranty(engine) 
    # ---------------------------
    
    print("=== TRANSFORMASI SELESAI ===")

if __name__ == "__main__":
    run_etl()