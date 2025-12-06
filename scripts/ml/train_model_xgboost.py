import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import joblib
import os

# --- KONFIGURASI ---
if os.path.exists('/opt/airflow/dags/scripts/ml'):
    base_dir = '/opt/airflow/dags/scripts/ml'
else:
    base_dir = './scripts/ml'
        
INPUT_FILE = os.path.join(base_dir, 'training_data_warranty.csv')
MODEL_DIR = os.path.join(base_dir, 'models')
os.makedirs(MODEL_DIR, exist_ok=True)

def train_xgboost():
    print("=== MEMULAI TRAINING MODEL (XGBOOST) ===")
    
    # 1. Load Data
    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"--> Data dimuat: {len(df)} baris.")
    except FileNotFoundError:
        print("[ERROR] File tidak ditemukan.")
        return

    # 2. Preprocessing
    # Hapus kolom identitas & data bocoran
    X = df.drop(columns=['is_claim', 'sales_key', 'product_name', 'quantity', 'unit_price'])
    y = df['is_claim']
    
    # Encoding
    print("--> Melakukan Encoding...")
    X = pd.get_dummies(X, drop_first=True)
    
    # 3. Hitung Scale Pos Weight (Untuk Imbalanced Data)
    # Rumus: Jumlah Negatif / Jumlah Positif
    count_neg = np.sum(y == 0)
    count_pos = np.sum(y == 1)
    scale_weight = count_neg / count_pos
    print(f"--> Ratio Data: {scale_weight:.2f} (Aman : Rusak)")
    
    # 4. Split Data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # 5. Training XGBoost
    print(f"--> Melatih XGBoost dengan scale_pos_weight={scale_weight:.2f}...")
    model = XGBClassifier(
        n_estimators=100,
        learning_rate=0.1,
        max_depth=5,
        scale_pos_weight=scale_weight, # KUNCI PENANGANAN IMBALANCED
        random_state=42,
        n_jobs=-1 # Gunakan semua core CPU
    )
    model.fit(X_train, y_train)
    
    # 6. Evaluasi
    print("--> Evaluasi Model:")
    y_pred = model.predict(X_test)
    
    acc = accuracy_score(y_test, y_pred)
    print(f"    Akurasi: {acc:.2f} ({acc*100:.1f}%)")
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred))
    
    # 7. Feature Importance
    print("\n--> Faktor Paling Berpengaruh (Top 5):")
    importances = pd.Series(model.feature_importances_, index=X.columns)
    print(importances.nlargest(5))
    
    # 8. Simpan Model
    model_path = os.path.join(MODEL_DIR, 'warranty_risk_xgboost.pkl')
    joblib.dump(model, model_path)
    print(f"\n--> Model disimpan di: {model_path}")

if __name__ == "__main__":
    train_xgboost()