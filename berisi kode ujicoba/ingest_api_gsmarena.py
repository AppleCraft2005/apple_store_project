import os
import json
import requests
import pandas as pd
from datetime import datetime
import time
import random

# --- KONFIGURASI API GSMARENA (Unofficial) ---
# URL ini adalah salah satu host publik yang umum. 
# Jika gagal, cek URL di Postman Anda dan ganti di sini.
BASE_URL = "https://gsmarena-api-opal.vercel.app" 

# --- KONFIGURASI PATH ---
if os.path.exists('/opt/airflow/datalake/raw'):
    SOURCE_CSV_DIR = '/opt/airflow/datalake/raw'
    LAKE_DEST_DIR = '/opt/airflow/datalake/raw/api_specs_gsmarena' # Folder beda biar ga kecampur
else:
    SOURCE_CSV_DIR = './datalake/raw'
    LAKE_DEST_DIR = './datalake/raw/api_specs_gsmarena'

def get_dummy_specs(product_name):
    """Fallback dummy jika API ini pun gagal"""
    return {
        "product_name": product_name,
        "source": "simulated_gsmarena",
        "specs": {
            "ram": "8GB", "camera": "12MP", "display": "6.1 inches"
        }
    }

def ingest_gsmarena():
    print(f"--- MEMULAI INGESTION API GSMARENA (BACKUP) ---")
    
    # 1. BACA FILE CSV PRODUK
    try:
        files = [f for f in os.listdir(SOURCE_CSV_DIR) if 'products' in f and f.endswith('.csv')]
        if not files:
            print("[ERROR] File products.csv tidak ditemukan.")
            return
        
        latest_file = sorted(files)[-1]
        file_path = os.path.join(SOURCE_CSV_DIR, latest_file)
        
        df = pd.read_csv(file_path, sep=',', encoding='utf-8-sig')
        df.columns = df.columns.str.strip()
        product_list = df['Product_Name'].unique()
        print(f"Target: {len(product_list)} produk dari {latest_file}")
        
    except Exception as e:
        print(f"[ERROR BACA CSV] {e}")
        return

    # 2. PROSES API
    os.makedirs(LAKE_DEST_DIR, exist_ok=True)
    collected_data = []

    for product_name in product_list:
        print(f"\nProcessing: {product_name}...")
        success = False
        
        try:
            # Bersihkan nama untuk query
            query = str(product_name).replace("Apple ", "").replace(" ", "+")
            
            # --- STEP 1: SEARCH ---
            # Endpoint: /search?q=...
            search_url = f"{BASE_URL}/search?q={query}"
            res_search = requests.get(search_url, timeout=15)
            
            if res_search.status_code == 200:
                data_search = res_search.json()
                
                # Cek hasil (biasanya array)
                if isinstance(data_search, list) and len(data_search) > 0:
                    first_match = data_search[0]
                    slug_id = first_match.get('id') or first_match.get('slug')
                    model_name = first_match.get('name')
                    
                    if slug_id:
                        print(f"   -> ID Ditemukan: {slug_id} ({model_name})")
                        
                        # --- STEP 2: GET DETAIL ---
                        # Endpoint: /brand/{id} atau langsung /{id} (tergantung versi API)
                        # Kita coba endpoint device umum
                        time.sleep(1) # Penting! Scraper gampang kena blokir
                        
                        # Coba pola URL detail umum untuk scraper ini
                        # Kadang returnnya langsung detail spec
                        # Kita asumsikan API ini butuh endpoint spesifik
                        detail_url = f"{BASE_URL}/device/{slug_id}"
                        res_detail = requests.get(detail_url, timeout=15)
                        
                        if res_detail.status_code == 200:
                            final_data = res_detail.json()
                            final_data['csv_reference'] = product_name
                            final_data['source_api'] = "gsmarena_unofficial"
                            
                            collected_data.append(final_data)
                            print(f"   -> [SUKSES] Detail tersimpan.")
                            success = True
                        else:
                            print(f"   -> [FAIL DETAIL] Status: {res_detail.status_code}")
                    else:
                        print("   -> [FAIL] Tidak ada ID di hasil search.")
                else:
                    print("   -> [INFO] Produk tidak ditemukan.")
            else:
                print(f"   -> [FAIL SEARCH] Status: {res_search.status_code}")

        except Exception as e:
            print(f"   -> [ERROR] {e}")

        # Fallback
        if not success:
            print("   -> [FALLBACK] Pakai data simulasi.")
            collected_data.append(get_dummy_specs(product_name))
            
        # Jeda lebih lama untuk unofficial API
        time.sleep(2)

    # 3. SIMPAN HASIL
    if collected_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"gsmarena_data_{timestamp}.json"
        dest_path = os.path.join(LAKE_DEST_DIR, filename)
        
        with open(dest_path, 'w') as f:
            json.dump(collected_data, f, indent=4)
        
        print(f"\n--- SELESAI. File JSON: {dest_path} ---")
    else:
        print("--- GAGAL TOTAL ---")

if __name__ == "__main__":
    ingest_gsmarena()