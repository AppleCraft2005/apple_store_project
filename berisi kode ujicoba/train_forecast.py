import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error
import matplotlib.pyplot as plt
import os

# --- KONFIGURASI PATH ---
if os.path.exists('/opt/airflow/dags/scripts/ml'):
    base_dir = '/opt/airflow/dags/scripts/ml'
else:
    base_dir = './scripts/ml'

INPUT_FILE = os.path.join(base_dir, 'training_data_forecast.csv')
MODEL_DIR = os.path.join(base_dir, 'models')

def train_forecast():
    print("=== MEMULAI TRAINING FORECAST (XGBOOST) ===")

    # 1. Load Data
    try:
        df = pd.read_csv(INPUT_FILE)
        # Set date sebagai index lagi (penting untuk grafik)
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
    except FileNotFoundError:
        print("[ERROR] File data tidak ditemukan.")
        return

    # 2. Split Data (Bukan Random!)
    # Untuk Time Series, kita TIDAK BOLEH acak. Harus potong berdasarkan waktu.
    # Misal: Latih pakai data Jan-Nov, Uji pakai data Des.
    split_point = int(len(df) * 0.8) # 80% awal untuk training

    train = df.iloc[:split_point]
    test = df.iloc[split_point:]

    # Pisahkan X (Fitur) dan y (Target)
    features = ['lag_1', 'lag_7', 'rolling_mean_7', 'day_of_week', 'day_of_month', 'month', 'is_weekend']
    target = 'total_qty'

    X_train = train[features]
    y_train = train[target]
    X_test = test[features]
    y_test = test[target]

    print(f"--> Data Train: {len(X_train)} hari | Data Test: {len(X_test)} hari")

    # 3. Training
    # PERBAIKAN: Pindahkan early_stopping_rounds ke dalam kurung XGBRegressor
    model = XGBRegressor(
        n_estimators=1000, 
        learning_rate=0.01, 
        early_stopping_rounds=50 
    )
    
    # Hapus early_stopping_rounds dari sini
    model.fit(X_train, y_train,
              eval_set=[(X_train, y_train), (X_test, y_test)],
              verbose=False)

    # 4. Prediksi & Evaluasi
    predictions = model.predict(X_test)

    mae = mean_absolute_error(y_test, predictions)
    rmse = np.sqrt(mean_squared_error(y_test, predictions))

    print(f"--> Error Rata-rata (MAE): {mae:.2f}")
    print(f"--> RMSE: {rmse:.2f}")
    print(f"--> Rata-rata Penjualan Asli: {y_test.mean():.2f}")

    # 5. Visualisasi Cepat (Pop-up window di laptop)
    # Hanya jalan kalau di local, di docker akan error display, jadi kita try-except
    try:
        plt.figure(figsize=(15, 5))
        plt.plot(test.index, y_test, label='Data Asli')
        plt.plot(test.index, predictions, label='Prediksi AI', color='red')
        plt.title('Sales Forecasting: Asli vs Prediksi')
        plt.legend()
        plt.show() # Akan muncul jendela grafik
    except:
        print("[INFO] Tidak bisa menampilkan grafik di environment ini (mungkin Docker).")

if __name__ == "__main__":
    train_forecast()