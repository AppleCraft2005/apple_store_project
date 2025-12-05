import os
import json
import requests
import pandas as pd
from datetime import datetime
import time

# --- KONFIGURASI API ---
API_URL = "https://script.google.com/macros/s/AKfycbxNu27V2Y2LuKUIQMK8lX1y0joB6YmG6hUwB1fNeVbgzEh22TcDGrOak03Fk3uBHmz-/exec"

# --- KONFIGURASI PATH ---
if os.path.exists('/opt/airflow/datalake/raw'):
    SOURCE_CSV_DIR = '/opt/airflow/datalake/raw'
    LAKE_DEST_DIR = '/opt/airflow/datalake/raw/api_specs'
else:
    SOURCE_CSV_DIR = './datalake/raw'
    LAKE_DEST_DIR = './datalake/raw/api_specs'

def ingest_gsmarena():
    print(f"--- MEMULAI INGESTION GSMARENA (DIRECT KEY) ---")
    
    # 1. BACA FILE CSV PRODUK
    try:
        files = [f for f in os.listdir(SOURCE_CSV_DIR) if 'products' in f and f.endswith('.csv')]
        if not files:
            print("[ERROR] File products.csv tidak ditemukan.")
            return
        
        latest_file = sorted(files)[-1]
        file_path = os.path.join(SOURCE_CSV_DIR, latest_file)
        print(f"Membaca file: {latest_file}")
        
        # Baca CSV
        df = pd.read_csv(file_path, sep=',', encoding='utf-8-sig')
        df.columns = df.columns.str.strip()
        
        # Cek apakah kolom kunci ada
        if 'gsmarena_key' not in df.columns:
            print("[ERROR] Kolom 'gsmarena_key' tidak ditemukan di CSV! Tambahkan dulu manual.")
            print(f"Kolom yang ada: {df.columns.tolist()}")
            return
            
    except Exception as e:
        print(f"[ERROR BACA CSV] {e}")
        return

    # 2. PROSES API
    os.makedirs(LAKE_DEST_DIR, exist_ok=True)
    collected_data = []
    
    # Header standar untuk POST JSON
    headers = {'Content-Type': 'application/json'}

    # Loop setiap baris di CSV
    for index, row in df.iterrows():
        product_name = row['Product_Name']
        gsm_key = str(row['gsmarena_key']) # Ambil key dari CSV
        
        # Cek apakah key valid (tidak nan/kosong)
        if pd.isna(row['gsmarena_key']) or gsm_key.strip() == '':
            print(f"Skipping {product_name}: Tidak ada gsmarena_key.")
            continue

        print(f"Processing: {product_name} (Key: {gsm_key})...", end=" ")
        
        try:
            # --- REQUEST POST ---
            payload = {
                "route": "device-detail",
                "key": gsm_key
            }
            
            response = requests.post(API_URL, json=payload, headers=headers, timeout=15)
            
            if response.status_code == 200:
                resp_json = response.json()
                
                # Cek status dari body response API (karena Google Script kadang return 200 walau error)
                if resp_json.get('status') != 200:
                     print(f"[API ERROR] {resp_json.get('message')}")
                     continue

                data_detail = resp_json.get('data', {})
                
                if data_detail:
                    # Tambahkan metadata kita
                    data_detail['csv_product_name'] = product_name
                    data_detail['source'] = "gsmarena_google_script"
                    
                    collected_data.append(data_detail)
                    print(f"[SUKSES]")
                else:
                    print(f"[KOSONG] Data tidak ditemukan.")
            else:
                print(f"[FAIL] HTTP Status: {response.status_code}")

        except Exception as e:
            print(f"\n[ERROR] {e}")
            
        # Jeda biar aman
        time.sleep(2)

    # 3. SIMPAN HASIL
    if collected_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"gsmarena_specs_{timestamp}.json"
        dest_path = os.path.join(LAKE_DEST_DIR, filename)
        
        with open(dest_path, 'w') as f:
            json.dump(collected_data, f, indent=4)
        
        print(f"\n--- SELESAI. File JSON: {dest_path} ---")
        print(f"Total data sukses: {len(collected_data)}")
    else:
        print("--- GAGAL TOTAL: Tidak ada data yang tersimpan ---")

if __name__ == "__main__":
    ingest_gsmarena()