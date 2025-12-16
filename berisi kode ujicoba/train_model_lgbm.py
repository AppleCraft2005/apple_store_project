import pandas as pd
from sklearn.model_selection import train_test_split
from lightgbm import LGBMClassifier # <--- INI BEDANYA
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix
import joblib
import os
import json
from datetime import datetime
import sys

# ... (Bagian Konfigurasi Path sama persis dengan script lain) ...
if os.path.exists('/opt/airflow/dags/scripts/ml'):
    base_dir = '/opt/airflow/dags/scripts/ml'
else:
    base_dir = './scripts/ml'

INPUT_FILE = os.path.join(base_dir, 'training_data_warranty.csv')
MODEL_DIR = os.path.join(base_dir, 'models')

def train_lgbm():
    print("=== MEMULAI TRAINING MODEL (LIGHTGBM) ===")
    
    # 1. Load Data
    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        print(f"[ERROR] File tidak ditemukan.")
        sys.exit(1)

    # 2. Preprocessing
    # ... (Sama persis dengan train_model.py lainnya) ...
    columns_to_drop = ['is_claim', 'sales_key', 'product_name', 'quantity', 'unit_price', 'total_amount']
    existing_cols = [c for c in columns_to_drop if c in df.columns]
    X = df.drop(columns=existing_cols)
    y = df['is_claim']
    
    X = pd.get_dummies(X, drop_first=True) # LightGBM sebenarnya bisa baca kategori langsung, tapi biar konsisten kita encode dulu
    
    # 3. Split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # 4. Training LightGBM
    print("--> Melatih LightGBM...")
    model = LGBMClassifier(
        n_estimators=100,
        learning_rate=0.1,
        random_state=42,
        class_weight='balanced', # Support balanced juga!
        verbose=-1
    )
    model.fit(X_train, y_train)
    
    # 5. Evaluasi
    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    report_dict = classification_report(y_test, y_pred, output_dict=True)
    cm = confusion_matrix(y_test, y_pred)
    
    print(f"    Akurasi: {acc:.2%}")
    print(classification_report(y_test, y_pred))
    
    # 6. Feature Importance
    print("\n--> Faktor Penentu:")
    feat_imp = pd.Series(model.feature_importances_, index=X.columns)
    print(feat_imp.nlargest(5))

    # 7. Simpan (Format Standar agar terbaca Dashboard)
    metrics_data = {
        "model_type": "LightGBM", # <--- GANTI INI
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "performance": {
            "accuracy": acc,
            "precision_class_1": report_dict['1']['precision'],
            "recall_class_1": report_dict['1']['recall'],
            "f1_score_class_1": report_dict['1']['f1-score'],
            "support_class_1": report_dict['1']['support']
        },
        "confusion_matrix": cm.tolist(),
        "hyperparameters": model.get_params()
    }
    
    with open(os.path.join(MODEL_DIR, 'metrics_active.json'), 'w') as f:
        json.dump(metrics_data, f, indent=4)
        
    joblib.dump(model, os.path.join(MODEL_DIR, 'model_active.pkl'))
    print("=== TRAINING SELESAI ===")

if __name__ == "__main__":
    train_lgbm()