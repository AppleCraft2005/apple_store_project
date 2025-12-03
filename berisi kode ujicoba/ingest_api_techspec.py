import os
import json
import requests
import pandas as pd
from datetime import datetime
import time
import random

# --- KONFIGURASI API ---
API_ID = "692d837d7e8e6352566c4628"
API_KEY = "bfd02add-8dd1-43e7-a880-d6ce15f24770"
SEARCH_URL = "https://api.techspecs.io/v5/products/search"
DETAIL_URL = "https://api.techspecs.io/v5/products"  # Nanti ditambah /{id}

# --- KONFIGURASI PATH ---
if os.path.exists('/opt/airflow/datalake/raw'):
    SOURCE_CSV_DIR = '/opt/airflow/datalake/raw'
    LAKE_DEST_DIR = '/opt/airflow/datalake/raw/api_specs'
else:
    SOURCE_CSV_DIR = './datalake/raw'
    LAKE_DEST_DIR = './datalake/raw/api_specs'

def get_dummy_specs(product_name):
    """Fungsi fallback: membuat data palsu"""
    return {
        "csv_product_name_reference": product_name,
        "source": "simulated_fallback",
        "specifications": {
            "hardware": {"ram": "8GB", "cpu": "Apple Bionic"},
            "camera": {"main": "12MP"},
            "display": {"size": "6.1 inches"}
        }
    }

def ingest_api_data():
    print(f"--- MEMULAI INGESTION API TECHSPECS (2-STEP) ---")
    
    # 1. BACA CSV
    product_list = []
    try:
        files = [f for f in os.listdir(SOURCE_CSV_DIR) if 'products' in f and f.endswith('.csv')]
        if not files:
            print(f"[ERROR] Tidak ada file products*.csv")
            return
        
        latest_file = sorted(files)[-1]
        product_file_path = os.path.join(SOURCE_CSV_DIR, latest_file)
        print(f"Membaca file: {latest_file}")
        
        df_products = pd.read_csv(product_file_path, sep=',', encoding='utf-8-sig')
        df_products.columns = df_products.columns.str.strip()
        product_list = df_products['Product_Name'].unique()
        
    except Exception as e:
        print(f"[ERROR FATAL] {e}")
        return

    # 2. LOOPING API
    os.makedirs(LAKE_DEST_DIR, exist_ok=True)
    
    headers = {
        "accept": "application/json",
        "x-api-id": API_ID,
        "x-api-key": API_KEY
    }
    
    collected_data = []

    for product_name in product_list:
        print(f"\nProcessing: {product_name}...")
        
        success_api = False
        try:
            # --- LANGKAH 1: CARI ID PRODUK ---
            query_name = str(product_name).replace("Apple ", "")
            search_params = {'query': query_name, 'page': 0, 'size': 10} # size 5 cukup
            
            # Request Search
            res_search = requests.get(SEARCH_URL, headers=headers, params=search_params, timeout=10)
            
            if res_search.status_code == 200:
                search_data = res_search.json()
                
                # Ambil ID dari hasil pertama
                if search_data['status'] == 'success' and len(search_data['data']) > 0:
                    first_item = search_data['data'][0]
                    product_id = first_item['Product']['id']
                    model_found = first_item['Product']['Model']
                    
                    print(f"   -> ID Ditemukan: {product_id} ({model_found})")
                    
                    # --- LANGKAH 2: AMBIL DETAIL SPESIFIKASI ---
                    time.sleep(1) # Jeda sebentar
                    res_detail = requests.get(f"{DETAIL_URL}/{product_id}", headers=headers, timeout=10)
                    
                    if res_detail.status_code == 200:
                        detail_data = res_detail.json()
                        
                        # Gabungkan info agar lengkap
                        detail_data['csv_product_name_reference'] = product_name
                        detail_data['source'] = "api_techspecs_v5"
                        
                        collected_data.append(detail_data)
                        print(f"   -> [SUKSES] Spesifikasi lengkap terambil.")
                        success_api = True
                    else:
                        print(f"   -> [FAIL] Gagal ambil detail. Status: {res_detail.status_code}")
                else:
                    print(f"   -> [INFO] Produk tidak ditemukan di pencarian.")
            else:
                print(f"   -> [FAIL] Search Error. Status: {res_search.status_code}")

        except Exception as e:
            print(f"   -> [ERROR] {e}")

        # Fallback ke Dummy
        if not success_api:
            print("   -> [FALLBACK] Menggunakan data simulasi.")
            collected_data.append(get_dummy_specs(product_name))
            
        time.sleep(1)

    # 3. SIMPAN
    if collected_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"techspecs_data_{timestamp}.json"
        dest_path = os.path.join(LAKE_DEST_DIR, filename)
        
        with open(dest_path, 'w') as f:
            json.dump(collected_data, f, indent=4)
        
        print(f"\n--- SELESAI. JSON tersimpan: {filename} ---")
        print(f"Total data: {len(collected_data)}")
    else:
        print("--- GAGAL TOTAL ---")

if __name__ == "__main__":
    ingest_api_data()