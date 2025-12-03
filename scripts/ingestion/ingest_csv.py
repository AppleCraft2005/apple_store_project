import os
import shutil
from datetime import datetime

# --- KONFIGURASI PATH OTOMATIS (Lokal vs Docker) ---
# Cek apakah kita berada di dalam container Docker (Folder /opt/airflow ada)
if os.path.exists('/opt/airflow/source_data'):
    print("Detected Environment: DOCKER CONTAINER")
    SOURCE_DIR = '/opt/airflow/source_data'
    LAKE_DIR = '/opt/airflow/datalake/raw'
else:
    print("Detected Environment: LOCAL MACHINE")
    # Asumsi script dijalankan dari root folder project
    SOURCE_DIR = './source_data'
    LAKE_DIR = './datalake/raw'

# Daftar file yang wajib ada
REQUIRED_FILES = [
    'sales.csv', 
    'products.csv', 
    'warranty.csv', 
    'customers.csv', 
    'employees.csv', 
    'stores.csv', 
    'category.csv'
]

def ingest_data():
    print(f"--- MEMULAI INGESTION CSV ---")
    print(f"Sumber Path : {os.path.abspath(SOURCE_DIR)}")
    print(f"Tujuan Path : {os.path.abspath(LAKE_DIR)}")

    # 1. Pastikan folder tujuan ada
    try:
        os.makedirs(LAKE_DIR, exist_ok=True)
    except Exception as e:
        print(f"[ERROR] Gagal membuat folder tujuan: {e}")
        return

    # 2. Loop setiap file yang dibutuhkan
    for filename in REQUIRED_FILES:
        source_path = os.path.join(SOURCE_DIR, filename)
        
        # Cek apakah file ada di sumber?
        if os.path.exists(source_path):
            # Buat nama file baru dengan Timestamp (agar tidak menimpa file lama/history)
            # Contoh: sales.csv -> sales_20251201_140000.csv
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Memisahkan nama file dan ekstensi dengan aman
            name, ext = os.path.splitext(filename)
            new_filename = f"{name}_{timestamp}{ext}"
            
            dest_path = os.path.join(LAKE_DIR, new_filename)
            
            try:
                # Salin file (Copy)
                shutil.copy2(source_path, dest_path)
                print(f"[SUKSES] Menyalin {filename} -> {new_filename}")
            except Exception as e:
                print(f"[ERROR] Gagal menyalin {filename}: {e}")
        else:
            print(f"[SKIP] File tidak ditemukan di sumber: {filename}")

    print(f"--- INGESTION SELESAI ---")

# Agar bisa dijalankan langsung untuk tes
if __name__ == "__main__":
    ingest_data()