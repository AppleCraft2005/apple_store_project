import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import joblib
import matplotlib.pyplot as plt
import seaborn as sns
import os

# --- KONFIGURASI ---
if os.path.exists('/opt/airflow/dags/scripts/ml'):
    base_dir = '/opt/airflow/dags/scripts/ml'
else:
    base_dir = './scripts/ml'
        
INPUT_FILE = os.path.join(base_dir, 'training_data_warranty.csv')
MODEL_DIR = os.path.join(base_dir, 'models')
os.makedirs(MODEL_DIR, exist_ok=True)

def train_warranty_model():
    print("=== MEMULAI TRAINING MODEL (WARRANTY RISK) ===")
    
    # 1. Load Data
    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"--> Data dimuat: {len(df)} baris.")
    except FileNotFoundError:
        print("[ERROR] File training data tidak ditemukan. Jalankan feature_engineering.py dulu!")
        return

    # 2. Preprocessing
    # UPDATE: Kita buang 'quantity' dan 'unit_price' agar model fokus ke SPEK PRODUK
    X = df.drop(columns=['is_claim', 'sales_key', 'product_name', 'quantity', 'unit_price']) 
    y = df['is_claim']
    
    # One-Hot Encoding untuk mengubah Teks (Chipset, Kategori) menjadi Angka
    print("--> Melakukan Encoding Data Kategorikal...")
    X = pd.get_dummies(X, drop_first=True)
    
    # Handle Missing Values (jika ada sisa) dengan 0
    X = X.fillna(0)
    
    # 3. Split Data (80% Latih, 20% Uji)
    print("--> Membagi Data Training & Testing (80/20)...")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # 4. Training Model
    print("--> Melatih Model Random Forest (Balanced)...")
    # UPDATE: Tambahkan class_weight='balanced'
    rf_model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
    rf_model.fit(X_train, y_train)
    
    # 5. Evaluasi
    print("--> Evaluasi Model:")
    y_pred = rf_model.predict(X_test)
    
    acc = accuracy_score(y_test, y_pred)
    print(f"    Akurasi: {acc:.2f} ({acc*100:.1f}%)")
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred))
    
    # 6. Feature Importance (Apa penyebab utama kerusakan?)
    # Ini SANGAT BAGUS untuk pembahasan Laporan UAS
    print("\n--> Faktor Paling Berpengaruh (Top 5):")
    feature_importances = pd.Series(rf_model.feature_importances_, index=X.columns)
    print(feature_importances.nlargest(5))
    
    # 7. Simpan Model
    model_path = os.path.join(MODEL_DIR, 'warranty_risk_model.pkl')
    joblib.dump(rf_model, model_path)
    print(f"\n--> Model disimpan di: {model_path}")
    
    print("=== TRAINING SELESAI ===")

if __name__ == "__main__":
    train_warranty_model()