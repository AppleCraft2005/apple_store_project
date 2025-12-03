import os
import json
import requests
import pandas as pd
from datetime import datetime
import time

# --- KONFIGURASI MOCK API ---
# Logika Cerdas: Cek apakah jalan di Docker atau Lokal
if os.path.exists('/.dockerenv'):
    # Jika di dalam Docker, panggil Host Machine
    BASE_URL = "http://host.docker.internal:8000/api/spec"
    print("[ENV] Terdeteksi berjalan di dalam DOCKER. Menghubungkan ke host.docker.internal...")
else:
    # Jika di Laptop biasa
    BASE_URL = "http://localhost:8000/api/spec"
    print("[ENV] Terdeteksi berjalan di LOCAL. Menghubungkan ke localhost...")

# --- KONFIGURASI PATH ---
if os.path.exists('/opt/airflow/datalake/raw'):
    SOURCE_CSV_DIR = '/opt/airflow/datalake/raw'
    LAKE_DEST_DIR = '/opt/airflow/datalake/raw/api_specs'
    # Jika jalan di dalam Docker, Localhost laptop = host.docker.internal
    # TAPI: Untuk testing manual sekarang (Terminal VSCode), pakai localhost biasa.
    # Nanti kalau dijalankan via Airflow, URL mungkin perlu disesuaikan.
else:
    SOURCE_CSV_DIR = './datalake/raw'
    LAKE_DEST_DIR = './datalake/raw/api_specs'


def ingest_api_data():
    print(f"--- MEMULAI INGESTION (INTERNAL MOCK API) ---")
    
    # 1. BACA CSV
    product_list = []
    try:
        files = [f for f in os.listdir(SOURCE_CSV_DIR) if 'products' in f and f.endswith('.csv')]
        if not files:
            print("[ERROR] File CSV produk tidak ditemukan.")
            return
        
        latest_file = sorted(files)[-1]
        file_path = os.path.join(SOURCE_CSV_DIR, latest_file)
        print(f"Target File: {latest_file}")
        
        df = pd.read_csv(file_path, sep=',', encoding='utf-8-sig')
        df.columns = df.columns.str.strip()
        product_list = df['Product_Name'].unique()
        
    except Exception as e:
        print(f"[ERROR BACA CSV] {e}")
        return

    # 2. REQUEST KE API LOKAL
    os.makedirs(LAKE_DEST_DIR, exist_ok=True)
    collected_data = []

    print(f"Connecting to Server: {BASE_URL} ...")

    for product_name in product_list:
        print(f"Fetching: {product_name}...", end=" ")
        
        try:
            # Kirim parameter ?name=Nama Produk
            params = {'name': product_name}
            response = requests.get(BASE_URL, params=params)
            
            if response.status_code == 200:
                data = response.json()
                # Ambil isi 'data' dari response API
                spec_data = data.get('data', {})
                
                # Tambahkan metadata source
                spec_data['csv_reference'] = product_name
                spec_data['source'] = "internal_mock_api"
                
                collected_data.append(spec_data)
                print(f"[OK]")
            else:
                print(f"[NOT FOUND] - Pastikan '{product_name}' ada di master_specs.json")

        except Exception as e:
            print(f"\n[CONNECTION ERROR] Server mati? Error: {e}")
            break # Berhenti jika server mati

    # 3. SIMPAN HASIL
    if collected_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"specs_internal_{timestamp}.json"
        dest_path = os.path.join(LAKE_DEST_DIR, filename)
        
        with open(dest_path, 'w') as f:
            json.dump(collected_data, f, indent=4)
        
        print(f"\n--- SUKSES. File: {dest_path} ---")
    else:
        print("\n--- GAGAL. Tidak ada data tersimpan. ---")

if __name__ == "__main__":
    ingest_api_data()