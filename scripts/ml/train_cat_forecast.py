import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import matplotlib.pyplot as plt
import os
import json
import joblib  # <--- Pastikan ini ada
from datetime import datetime
import sys

# --- KONFIGURASI PATH ---
if os.path.exists('/opt/airflow/dags/scripts/ml'):
    base_dir = '/opt/airflow/dags/scripts/ml'
else:
    base_dir = './scripts/ml'

INPUT_FILE = os.path.join(base_dir, 'train_data_cat_forecast.csv')
MODEL_DIR = os.path.join(base_dir, 'models')
GRAPH_DIR = os.path.join(base_dir, 'graphs')
os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(GRAPH_DIR, exist_ok=True)

def train_category_forecast():
    print("=== TRAINING FORECAST PER KATEGORI (XGBOOST) ===")
    
    # 1. Load Data
    try:
        df = pd.read_csv(INPUT_FILE)
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        print(f"--> Data dimuat: {len(df)} hari.")
    except FileNotFoundError:
        print("[ERROR] File data tidak ditemukan.")
        return
    
    feature_cols = [c for c in df.columns if '_lag' in c or '_roll' in c or c in ['day_of_week', 'month']]
    target_categories = [c for c in df.columns if c not in feature_cols]
    
    metrics_report = {
        "model_type": "XGBoost Regressor (Multi-Category)",
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "overall": {},
        "categories": {}
    }
    
    total_mae = 0
    
    # 2. Loop Training
    for target in target_categories:
        print(f"\n--- Training Model: {target} ---")
        
        features = [f'{target}_lag1', f'{target}_roll7', 'day_of_week', 'month']
        X = df[features]
        y = df[target]
        
        split_point = int(len(df) * 0.9)
        X_train, X_test = X.iloc[:split_point], X.iloc[split_point:]
        y_train, y_test = y.iloc[:split_point], y.iloc[split_point:]
        
        model = XGBRegressor(n_estimators=500, learning_rate=0.05, n_jobs=-1)
        model.fit(X_train, y_train)
        
        pred = model.predict(X_test)
        
        # Evaluasi
        mae = mean_absolute_error(y_test, pred)
        rmse = np.sqrt(mean_squared_error(y_test, pred))
        avg_sales = y_test.mean()
        
        # Hitung MAPE & Akurasi
        df_eval = pd.DataFrame({'y_true': y_test, 'y_pred': pred})
        df_eval = df_eval[df_eval['y_true'] > 0]
        
        if not df_eval.empty:
            mape = np.mean(np.abs(df_eval['y_true'] - df_eval['y_pred']) / df_eval['y_true']) * 100
            accuracy_pct = max(0, 100 - mape)
        else:
            mape = 0
            accuracy_pct = 0
            
        error_rate_pct = (mae / avg_sales * 100) if avg_sales > 0 else 0
        
        print(f"    MAE: {mae:.1f} | Akurasi: {accuracy_pct:.2f}%")
        
        total_mae += mae
        
        # --- UPDATE BARU: SIMPAN MODEL PER KATEGORI ---
        # Nama file aman (ganti spasi dengan underscore)
        safe_name = target.replace(" ", "_")
        model_filename = f'forecast_model_{safe_name}.pkl'
        model_path = os.path.join(MODEL_DIR, model_filename)
        
        joblib.dump(model, model_path)
        # ----------------------------------------------
        
        # Simpan ke Dictionary Metrics
        metrics_report["categories"][target] = {
            "model_file": model_filename, # Simpan referensi nama file juga
            "mae": round(mae, 2),
            "rmse": round(rmse, 2),
            "avg_sales": round(avg_sales, 2),
            "mape_pct": round(mape, 2),
            "accuracy_pct": round(accuracy_pct, 2),
            "error_rate_pct": round(error_rate_pct, 1)
        }

        # Plotting
        try:
            plt.figure(figsize=(12, 5))
            plt.plot(y_test.index, y_test, label='Aktual', alpha=0.7)
            plt.plot(y_test.index, pred, label='Prediksi', color='red', linestyle='--')
            plt.title(f"{target} - Akurasi: {accuracy_pct:.1f}%")
            plt.legend()
            plt.savefig(os.path.join(GRAPH_DIR, f'forecast_{target}.png'))
            plt.close()
        except:
            pass

    metrics_report["overall"]["avg_mae"] = round(total_mae / len(target_categories), 2)
    
    # 3. Simpan JSON
    metrics_path = os.path.join(MODEL_DIR, 'metrics_forecast.json')
    with open(metrics_path, 'w') as f:
        json.dump(metrics_report, f, indent=4)

    print(f"\n--> [DONE] Models & Metrics tersimpan di: {MODEL_DIR}")

if __name__ == "__main__":
    train_category_forecast()