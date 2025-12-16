import pandas as pd
import numpy as np
from xgboost import XGBRegressor
import matplotlib.pyplot as plt
import os
from datetime import timedelta
import joblib

# --- KONFIGURASI ---
if os.path.exists('/opt/airflow/dags/scripts/ml'):
    base_dir = '/opt/airflow/dags/scripts/ml'
else:
    base_dir = './scripts/ml'

INPUT_FILE = os.path.join(base_dir, 'train_data_cat_forecast.csv')
FUTURE_DAYS = 180

def predict_future():
    print(f"=== MEMULAI FORECASTING {FUTURE_DAYS} HARI KE DEPAN ===")
    
    # 1. Load Data
    try:
        df = pd.read_csv(INPUT_FILE)
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
    except FileNotFoundError:
        print("[ERROR] File data tidak ditemukan. Jalankan feature_engineering_cat_forecast.py dulu.")
        return

    # Identifikasi Kategori
    feature_cols = [c for c in df.columns if '_lag' in c or '_roll' in c or c in ['day_of_week', 'month']]
    target_categories = [c for c in df.columns if c not in feature_cols]
    
    print(f"--> Kategori: {target_categories}")
    
    future_predictions = []

    # 2. Loop Per Kategori
    for cat in target_categories:
        print(f"--> Meramal: {cat}...")
        
        # Siapkan Data Latih
        features = [f'{cat}_lag1', f'{cat}_roll7', 'day_of_week', 'month']
        X = df[features]
        y = df[cat]
        
        # Latih Model Full (Semua data)
        model = XGBRegressor(n_estimators=500, learning_rate=0.05, n_jobs=-1)
        model.fit(X, y)
        
        # --- LOGIKA RECURSIVE FORECASTING ---
        # Ambil data terakhir sebagai titik awal
        last_row = df.iloc[-1].copy()
        current_date = df.index[-1]
        
        # List untuk menyimpan sejarah penjualan (termasuk prediksi) buat hitung rolling
        # Ambil 7 hari terakhir data asli
        history_sales = df[cat].tail(7).tolist()
        
        for i in range(FUTURE_DAYS):
            next_date = current_date + timedelta(days=i+1)
            
            # 1. Buat Fitur Baru untuk Besok
            # Lag 1 = Penjualan Terakhir (yang baru saja diprediksi/terjadi)
            feat_lag1 = history_sales[-1]
            # Rolling 7 = Rata-rata 7 hari terakhir
            feat_roll7 = np.mean(history_sales[-7:])
            
            # Buat DataFrame satu baris
            X_future = pd.DataFrame({
                f'{cat}_lag1': [feat_lag1],
                f'{cat}_roll7': [feat_roll7],
                'day_of_week': [next_date.dayofweek],
                'month': [next_date.month]
            })
            
            # 2. Prediksi
            pred_val = model.predict(X_future)[0]
            
            # Jangan biarkan negatif (penjualan gak mungkin minus)
            if pred_val < 0: pred_val = 0
            
            # 3. Masukkan hasil prediksi ke history (untuk dipake forecasting besoknya lagi)
            history_sales.append(pred_val)
            
            # Simpan hasil
            future_predictions.append({
                'date': next_date,
                'category': cat,
                'predicted_units': int(pred_val) # Bulatkan jadi integer
            })

    # 3. Simpan Hasil ke CSV
    df_future = pd.DataFrame(future_predictions)
    output_path = os.path.join(base_dir, 'future_sales_forecast.csv')
    df_future.to_csv(output_path, index=False)
    
    print(f"--> Hasil ramalan disimpan di: {output_path}")
    print("\n--- CONTOH HASIL (5 Baris Pertama) ---")
    print(df_future.head())
    
    # 4. Bikin Grafik Simpel (Optional)
    try:
        pivot_chart = df_future.pivot(index='date', columns='category', values='predicted_units')
        pivot_chart.plot(figsize=(12, 6), marker='o')
        plt.title(f"Prediksi Penjualan {FUTURE_DAYS} Hari ke Depan")
        plt.grid(True, alpha=0.3)
        plt.savefig(os.path.join(base_dir, 'graphs', 'future_forecast_plot.png'))
        print("--> Grafik disimpan.")
    except:
        pass

if __name__ == "__main__":
    predict_future()