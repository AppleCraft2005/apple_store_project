# import pandas as pd
# from sqlalchemy import create_engine
# import os

# # --- KONFIGURASI KONEKSI (Auto Detect) ---
# if os.path.exists('/.dockerenv'):
#     DB_HOST = 'apple_store_postgres'
#     OUTPUT_DIR = '/opt/airflow/dags/scripts/ml'
# else:
#     DB_HOST = 'localhost'
#     OUTPUT_DIR = './scripts/ml'

# DB_USER = 'admin'
# DB_PASS = 'admin'
# DB_NAME = 'apple_dwh'
# DB_PORT = '5432'
# CONN_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# def generate_category_forecast_data():
#     print("=== FEATURE ENGINEERING: CATEGORY FORECAST ===")
    
#     try:
#         engine = create_engine(CONN_STRING)
        
#         # 1. Query: Ambil Total Qty per Hari per Kategori
#         query = """
#         SELECT 
#             d.full_date as date,
#             c.category_name,
#             SUM(f.quantity) as total_qty
#         FROM dwh.fact_sales f
#         JOIN dwh.dim_date d ON f.date_key = d.date_key
#         JOIN dwh.dim_product p ON f.product_key = p.product_key
#         JOIN dwh.dim_category c ON p.category_key = c.category_key
#         GROUP BY d.full_date, c.category_name
#         ORDER BY d.full_date
#         """
#         print("--> Mengambil data dari DWH...")
#         df = pd.read_sql(query, engine)
        
#         if df.empty:
#             print("[ERROR] Data kosong. Cek apakah Fact Sales sudah terisi.")
#             return

#         # --- 1.5 FIX PENTING: Konversi ke Datetime ---
#         # Ini agar index nanti dikenali sebagai tanggal, bukan teks
#         df['date'] = pd.to_datetime(df['date'])

#         # 2. Pivot Data (Agar setiap kategori jadi kolom sendiri)
#         # Bentuk: [Date, Smartphone, Laptop, Tablet, ...]
#         print("--> Melakukan Pivot Data...")
#         df_pivot = df.pivot(index='date', columns='category_name', values='total_qty').fillna(0)
        
#         # 3. Feature Engineering (Lag per Kategori)
#         categories = df_pivot.columns.tolist()
#         print(f"--> Kategori ditemukan: {categories}")
        
#         df_final = df_pivot.copy()
        
#         for cat in categories:
#             # Penjualan kategori X kemarin (Lag 1)
#             df_final[f'{cat}_lag1'] = df_pivot[cat].shift(1)
#             # Rata-rata penjualan kategori X seminggu terakhir (Rolling 7)
#             df_final[f'{cat}_roll7'] = df_pivot[cat].rolling(window=7).mean()
            
#         # Tambahkan fitur tanggal (Sekarang pasti berhasil karena index sudah datetime)
#         df_final['day_of_week'] = df_final.index.dayofweek
#         df_final['month'] = df_final.index.month
        
#         # Hapus baris kosong di awal akibat shift/rolling
#         df_final = df_final.dropna()
        
#         # Reset index agar tanggal jadi kolom biasa
#         df_final = df_final.reset_index()
        
#         # Simpan
#         output_path = os.path.join(OUTPUT_DIR, 'train_data_cat_forecast.csv')
        
#         # Pastikan folder output ada
#         os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
#         df_final.to_csv(output_path, index=False)
#         print(f"--> Data siap latih disimpan di: {output_path}")

#     except Exception as e:
#         print(f"[FATAL ERROR] {e}")
#         # Print error detail untuk debugging
#         import traceback
#         traceback.print_exc()

# if __name__ == "__main__":
#     generate_category_forecast_data()

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os
import sys
from scipy.stats import ks_2samp
import matplotlib.pyplot as plt # <--- Import untuk menggambar

# --- KONFIGURASI KONEKSI DATABASE ---
DB_USER = 'admin'
DB_PASSWORD = 'admin'
DB_HOST = 'apple_store_postgres'
DB_PORT = '5432'
DB_NAME = 'apple_dwh'

# Path Output Data Latih
if os.path.exists('/opt/airflow/dags/scripts/ml'):
    base_dir = '/opt/airflow/dags/scripts/ml'
else:
    base_dir = './scripts/ml'

OUTPUT_FILE = os.path.join(base_dir, 'train_data_cat_forecast.csv')
# Path Output Gambar (Akan dibuat otomatis)
PLOT_DIR = os.path.join(base_dir, 'quality_plots')

def save_outlier_plots(df, target_col, outliers, lower_bound, upper_bound):
    """
    Fungsi khusus untuk men-generate gambar Boxplot & Scatterplot
    dan menyimpannya ke disk.
    """
    try:
        # Buat folder jika belum ada
        os.makedirs(PLOT_DIR, exist_ok=True)
        
        # --- GAMBAR 1: BOXPLOT ---
        plt.figure(figsize=(10, 6))
        plt.boxplot(df[target_col], vert=False)
        plt.title(f'Distribusi Outlier: {target_col} (Metode IQR)')
        plt.xlabel('Quantity')
        
        # Simpan Gambar 1
        boxplot_path = os.path.join(PLOT_DIR, 'outlier_boxplot.png')
        plt.savefig(boxplot_path)
        plt.close() # Tutup agar memori hemat
        
        # --- GAMBAR 2: TIME SERIES SCATTER ---
        plt.figure(figsize=(12, 6))
        
        # Plot Data Normal (Biru)
        normal_data = df[~df.index.isin(outliers.index)]
        plt.scatter(normal_data['date'], normal_data[target_col], 
                   c='blue', alpha=0.5, label='Normal', s=10)
        
        # Plot Data Outlier (Merah)
        plt.scatter(outliers['date'], outliers[target_col], 
                   c='red', label='Outlier', s=50, marker='x')
        
        # Garis Batas
        plt.axhline(y=upper_bound, color='green', linestyle='--', label='Batas Atas IQR')
        plt.axhline(y=lower_bound, color='green', linestyle='--', label='Batas Bawah IQR')
        
        plt.title(f'Deteksi Waktu Terjadinya Outlier')
        plt.xlabel('Tanggal')
        plt.ylabel('Quantity')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Simpan Gambar 2
        scatter_path = os.path.join(PLOT_DIR, 'outlier_scatter.png')
        plt.savefig(scatter_path)
        plt.close()
        
        print(f"       [INFO] 📸 Gambar bukti outlier berhasil disimpan di:")
        print(f"              1. {boxplot_path}")
        print(f"              2. {scatter_path}")
        
    except Exception as e:
        print(f"       [WARNING] Gagal menyimpan gambar: {e}")

def print_data_quality_report(df, target_col='quantity'):
    print("\n" + "="*60)
    print("   📊  DATA QUALITY REPORT (QUALITY METRICS)   ")
    print("="*60)
    
    total_rows = len(df)
    
    # [1] MISSING VALUES
    missing_count = df[target_col].isnull().sum()
    print(f"\n[1] MISSING VALUES: {missing_count} baris")

    # [2] OUTLIERS (Metode IQR)
    Q1 = df[target_col].quantile(0.25)
    Q3 = df[target_col].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    outliers = df[(df[target_col] < lower_bound) | (df[target_col] > upper_bound)]
    outlier_count = len(outliers)
    outlier_pct = (outlier_count / total_rows) * 100
    
    print(f"\n[2] OUTLIERS (Pencilan - Metode IQR)")
    print(f"    - Batas Bawah: {lower_bound:.2f} | Batas Atas: {upper_bound:.2f}")
    print(f"    - Jumlah Outliers: {outlier_count} baris ({outlier_pct:.2f}%)")
    
    if outlier_count > 0:
        print("    -> DETEKSI LOKASI OUTLIER (Top 5 Ekstrim):")
        top_outliers = outliers.sort_values(by=target_col, ascending=False).head(5)
        for idx, row in top_outliers.iterrows():
            tgl = str(row['date'])[:10] 
            kat = row['category_name']
            qty = row[target_col]
            print(f"       * Tanggal: {tgl} | Kategori: {kat:<15} | Qty: {qty}")
        
        # --- TRIGGER GENERATE GAMBAR ---
        print("    -> GENERATING PLOTS...")
        save_outlier_plots(df, target_col, outliers, lower_bound, upper_bound)
        # -------------------------------
    
    print("    -> HANDLING: Dibiarkan (XGBoost Robust terhadap Outlier)")

    # [3] DATA DRIFT (KS Test)
    split_idx = int(total_rows * 0.5)
    past_data = df.iloc[:split_idx][target_col]
    current_data = df.iloc[split_idx:][target_col]
    
    ks_stat, p_value = ks_2samp(past_data, current_data)
    
    print(f"\n[3] DATA DRIFT (Metode: Kolmogorov-Smirnov Test)")
    print(f"    - P-Value: {p_value:.4f}")
    if p_value < 0.05:
        print("    -> STATUS: DRIFT TERDETEKSI")
    else:
        print("    -> STATUS: STABIL")
        
    print("="*60 + "\n")

def run_feature_engineering():
    print("--> Memulai Koneksi ke Data Warehouse...")
    
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    try:
        engine = create_engine(db_url)
        conn = engine.connect()
    except Exception as e:
        print(f"[ERROR] Gagal konek DB: {e}")
        return

    print("--> Mengambil data penjualan harian...")
    query = """
    SELECT 
        dd.full_date as date,
        dc.category_name,
        SUM(fs.quantity) as quantity
    FROM dwh.fact_sales fs
    JOIN dwh.dim_date dd ON fs.date_key = dd.date_key
    JOIN dwh.dim_product dp ON fs.product_key = dp.product_key
    JOIN dwh.dim_category dc ON dp.category_key = dc.category_key
    GROUP BY dd.full_date, dc.category_name
    ORDER BY dd.full_date;
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Cetak Laporan & Generate Gambar
    print_data_quality_report(df, target_col='quantity')

    # Proses Pivot & Feature Engineering
    print("--> Melakukan Pivot Table & Handling Missing Values...")
    df_pivot = df.pivot(index='date', columns='category_name', values='quantity').fillna(0)
    df_pivot.index = pd.to_datetime(df_pivot.index)
    df_pivot = df_pivot.resample('D').sum().fillna(0) 

    print("--> Membuat Fitur Tambahan (Lag & Rolling)...")
    for col in df_pivot.columns:
        df_pivot[f'{col}_lag1'] = df_pivot[col].shift(1)
        df_pivot[f'{col}_roll7'] = df_pivot[col].rolling(window=7).mean()
    
    df_pivot['day_of_week'] = df_pivot.index.dayofweek
    df_pivot['month'] = df_pivot.index.month
    
    df_final = df_pivot.dropna()
    
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df_final.to_csv(OUTPUT_FILE)
    print(f"--> [SUCCESS] Data siap latih tersimpan di: {OUTPUT_FILE}")

if __name__ == "__main__":
    run_feature_engineering()