# import pandas as pd
# import numpy as np
# from sklearn.model_selection import train_test_split
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.metrics import classification_report, accuracy_score, confusion_matrix
# import joblib
# import os
# import json
# from datetime import datetime
# import sys

# # --- KONFIGURASI PATH ---
# if os.path.exists('/opt/airflow/dags/scripts/ml'):
#     base_dir = '/opt/airflow/dags/scripts/ml' # Path Docker
# else:
#     base_dir = './scripts/ml' # Path Lokal

# INPUT_FILE = os.path.join(base_dir, 'training_data_warranty.csv')
# MODEL_DIR = os.path.join(base_dir, 'models')
# os.makedirs(MODEL_DIR, exist_ok=True)

# def train_rf_model():
#     print("=== MEMULAI TRAINING MODEL (RANDOM FOREST) ===")
    
#     # 1. LOAD DATA
#     try:
#         df = pd.read_csv(INPUT_FILE)
#         print(f"--> Data dimuat: {len(df)} baris.")
#     except FileNotFoundError:
#         print(f"[ERROR] File {INPUT_FILE} tidak ditemukan. Jalankan feature_engineering.py dulu!")
#         sys.exit(1)

#     # 2. PREPROCESSING
#     # Drop kolom identitas & data bocoran (quantity/price)
#     columns_to_drop = ['is_claim', 'sales_key', 'product_name', 'quantity', 'unit_price', 'total_amount']
#     existing_cols = [c for c in columns_to_drop if c in df.columns]
    
#     X = df.drop(columns=existing_cols)
#     y = df['is_claim']
    
#     print("--> Melakukan Encoding Data Kategorikal...")
#     X = pd.get_dummies(X, drop_first=True)
#     X = X.fillna(0) # Handle missing values jika ada
    
#     # 3. SPLIT DATA
#     print("--> Membagi Data Training & Testing (80/20)...")
#     X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
#     # 4. TRAINING
#     print("--> Melatih Model Random Forest (Balanced)...")
#     # n_estimators=100: Jumlah pohon
#     # class_weight='balanced': Memberi bobot lebih pada kelas minoritas (Klaim)
#     rf_model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced', n_jobs=-1)
#     rf_model.fit(X_train, y_train)
    
#     # 5. EVALUASI
#     print("--> Evaluasi Model...")
#     y_pred = rf_model.predict(X_test)
    
#     acc = accuracy_score(y_test, y_pred)
#     report_dict = classification_report(y_test, y_pred, output_dict=True)
#     cm = confusion_matrix(y_test, y_pred)
    
#     # Tampilkan di Terminal
#     print(f"    Akurasi: {acc:.2%}")
#     print("\nClassification Report:")
#     print(classification_report(y_test, y_pred))
    
#     # 6. FEATURE IMPORTANCE
#     print("\n--> Faktor Penentu Risiko Terbesar (Top 5):")
#     feat_importances = pd.Series(rf_model.feature_importances_, index=X.columns)
#     print(feat_importances.nlargest(5))

#     # 7. SIMPAN HASIL (CHAMPION MODEL FORMAT)
#     metrics_data = {
#         "model_type": "Random Forest",
#         "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#         "performance": {
#             "accuracy": acc,
#             "precision_class_1": report_dict['1']['precision'],
#             "recall_class_1": report_dict['1']['recall'],
#             "f1_score_class_1": report_dict['1']['f1-score'],
#             "support_class_1": report_dict['1']['support']
#         },
#         "confusion_matrix": cm.tolist(),
#         "hyperparameters": rf_model.get_params()
#     }
    
#     # Simpan Metrics JSON
#     metrics_path = os.path.join(MODEL_DIR, 'metrics_active.json')
#     with open(metrics_path, 'w') as f:
#         json.dump(metrics_data, f, indent=4)
        
#     # Simpan Model PKL
#     model_path = os.path.join(MODEL_DIR, 'model_active.pkl')
#     joblib.dump(rf_model, model_path)
    
#     print(f"\n--> [RF] Model AKTIF diperbarui: {model_path}")
#     print("=== TRAINING SELESAI ===")

# if __name__ == "__main__":
#     train_rf_model()

# import pandas as pd
# import numpy as np
# from sklearn.model_selection import train_test_split
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.metrics import classification_report, accuracy_score, confusion_matrix
# import joblib
# import os
# import json
# from datetime import datetime
# import sys

# # --- KONFIGURASI PATH ---
# # Deteksi otomatis apakah di Docker atau Laptop
# if os.path.exists('/opt/airflow/dags/scripts/ml'):
#     base_dir = '/opt/airflow/dags/scripts/ml'
# else:
#     base_dir = './scripts/ml'

# INPUT_FILE = os.path.join(base_dir, 'training_data_warranty.csv')
# MODEL_DIR = os.path.join(base_dir, 'models')
# os.makedirs(MODEL_DIR, exist_ok=True)

# def train_rf_model():
#     print("=== MEMULAI TRAINING MODEL (RANDOM FOREST) ===")
    
#     # 1. LOAD DATA
#     try:
#         df = pd.read_csv(INPUT_FILE)
#         print(f"--> Data dimuat: {len(df)} baris.")
#     except FileNotFoundError:
#         print(f"[ERROR] File {INPUT_FILE} tidak ditemukan. Jalankan feature_engineering.py dulu!")
#         sys.exit(1)

#     # 2. PREPROCESSING
#     # Drop kolom identitas.
#     # Note: Kali ini kita PERTAHANKAN 'price' sebagai fitur, jadi jangan di-drop!
#     columns_to_drop = ['is_claim', 'sales_key', 'product_name'] # Hapus price/quantity dari sini
    
#     existing_cols = [c for c in columns_to_drop if c in df.columns]
#     X = df.drop(columns=existing_cols)
#     y = df['is_claim']
    
#     print("--> Melakukan Encoding Data Kategorikal...")
#     # Ubah teks (seperti Chipset A15) menjadi angka (One-Hot Encoding)
#     X = pd.get_dummies(X, drop_first=True)
#     X = X.fillna(0) # Handle missing values jika ada
    
#     # 3. SPLIT DATA
#     print("--> Membagi Data Training & Testing (80/20)...")
#     X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
#     # 4. TRAINING
#     print("--> Melatih Model Random Forest (Balanced)...")
#     # n_estimators=100: Jumlah pohon
#     # class_weight='balanced': Memberi bobot lebih pada kelas minoritas (Klaim) agar Recall tinggi
#     # n_jobs=-1: Pakai semua core CPU biar ngebut
#     rf_model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced', n_jobs=-1)
#     rf_model.fit(X_train, y_train)
    
#     # 5. EVALUASI
#     print("--> Evaluasi Model...")
#     y_pred = rf_model.predict(X_test)
    
#     acc = accuracy_score(y_test, y_pred)
#     report_dict = classification_report(y_test, y_pred, output_dict=True)
#     cm = confusion_matrix(y_test, y_pred)
    
#     # Tampilkan di Terminal
#     print(f"    Akurasi: {acc:.2%}")
#     print("\nClassification Report:")
#     print(classification_report(y_test, y_pred))
    
#     # 6. FEATURE IMPORTANCE (Analisis Faktor Penyebab)
#     print("\n--> Faktor Penentu Risiko Terbesar (Top 5):")
#     feat_importances = pd.Series(rf_model.feature_importances_, index=X.columns)
#     print(feat_importances.nlargest(5))


#     # 7. SIMPAN HASIL (UPDATE: Tambah Full Report)
#     metrics_data = {
#         "model_type": "Random Forest",
#         "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#         "performance": {
#             "accuracy": acc,
#             # Kita tetap simpan ini untuk KPI Card di atas
#             "precision_class_1": report_dict['1']['precision'],
#             "recall_class_1": report_dict['1']['recall'],
#             "f1_score_class_1": report_dict['1']['f1-score']
#         },
#         # --- TAMBAHAN BARU: Simpan seluruh laporan ---
#         "full_report": report_dict, 
#         # ---------------------------------------------
#         "confusion_matrix": cm.tolist(),
#         "hyperparameters": rf_model.get_params()
#     }
    
#     # ... (kode simpan file sama) ...
    
#     # Simpan Metrics JSON
#     metrics_path = os.path.join(MODEL_DIR, 'metrics_active.json')
#     with open(metrics_path, 'w') as f:
#         json.dump(metrics_data, f, indent=4)
        
#     # Simpan Model PKL
#     model_path = os.path.join(MODEL_DIR, 'model_active.pkl')
#     joblib.dump(rf_model, model_path)
    
#     print(f"\n--> [RF] Model AKTIF diperbarui: {model_path}")
#     print("=== TRAINING SELESAI ===")

# if __name__ == "__main__":
#     train_rf_model()

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    classification_report, 
    accuracy_score, 
    confusion_matrix,
    roc_auc_score
)
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import joblib
import json
import os
from datetime import datetime

# --- PATH ---
BASE_DIR = "/opt/airflow/dags/scripts/ml" if os.path.exists("/opt/airflow") else "./scripts/ml"
INPUT_FILE = os.path.join(BASE_DIR, "training_data_warranty.csv")
MODEL_DIR = os.path.join(BASE_DIR, "models")
os.makedirs(MODEL_DIR, exist_ok=True)

def train_best_rf():
    print("=== TRAINING RANDOM FOREST (BEST PRACTICE) ===")

    # 1. LOAD DATA
    df = pd.read_csv(INPUT_FILE)
    print(f"[INFO] Loaded dataset: {df.shape[0]} rows")

    # 2. SPLIT X & y
    target_col = "is_claim"
    y = df[target_col]
    X = df.drop(columns=[target_col, "sales_key", "product_name"])  # Drop non-feature columns

    # Identify categorical & numerical columns
    categorical_cols = X.select_dtypes(include=["object"]).columns
    numeric_cols = X.select_dtypes(include=["int64", "float64"]).columns

    print(f"[INFO] Numerical features: {list(numeric_cols)}")
    print(f"[INFO] Categorical features: {list(categorical_cols)}")

    # 3. PREPROCESSING PIPELINE
    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_cols),
            ("num", "passthrough", numeric_cols)
        ]
    )

    # 4. BASE RANDOM FOREST
    rf = RandomForestClassifier(
        random_state=42,
        class_weight=None,   # Karena dataset sudah balanced
        n_jobs=-1
    )

    # 5. HYPERPARAMETER TUNING (BEST PRACTICE)
    param_grid = {
        "rf__n_estimators": [150, 250, 350, 500],
        "rf__max_depth": [10, 20, 30, None],
        "rf__min_samples_split": [2, 5, 10],
        "rf__min_samples_leaf": [1, 2, 4],
        "rf__max_features": ["sqrt", "log2"],
    }

    pipeline = Pipeline(steps=[
        ("preprocess", preprocessor),
        ("rf", rf)
    ])

    search = RandomizedSearchCV(
        estimator=pipeline,
        param_distributions=param_grid,
        n_iter=20, 
        cv=3,
        n_jobs=-1,
        scoring="f1",
        verbose=1,
        random_state=42
    )

    # 6. TRAIN
    print("[INFO] Training with RandomizedSearchCV...")
    search.fit(X, y)

    best_model = search.best_estimator_
    print(f"[INFO] Best parameters: {search.best_params_}")

    # 7. EVALUATION
    print("[INFO] Evaluating model...")
    y_pred = best_model.predict(X)
    y_prob = best_model.predict_proba(X)[:, 1]

    acc = accuracy_score(y, y_pred)
    auc = roc_auc_score(y, y_prob)
    report = classification_report(y, y_pred, output_dict=True)
    cm = confusion_matrix(y, y_pred)

    print(f"Accuracy: {acc:.4f}")
    print(f"ROC-AUC: {auc:.4f}")
    print("\nClassification Report:")
    print(classification_report(y, y_pred))

    # 8. FEATURE IMPORTANCE (convert from RF inside pipeline)
    rf_model = best_model.named_steps["rf"]
    ohe = best_model.named_steps["preprocess"].named_transformers_["cat"]

    # Build feature names list
    ohe_features = ohe.get_feature_names_out(categorical_cols)
    all_features = np.concatenate([ohe_features, numeric_cols])

    importances = pd.Series(rf_model.feature_importances_, index=all_features).sort_values(ascending=False)

    print("\nTop 10 Feature Importances:")
    print(importances.head(10))

    # 9. SAVE MODEL
    model_path = os.path.join(MODEL_DIR, "rf_model_best.pkl")
    joblib.dump(best_model, model_path)

    print(f"[INFO] Model saved: {model_path}")

    # 10. SAVE METRICS
    metrics = {
        "model_type": "RandomForest-BestTuned",
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "accuracy": float(acc),
        "roc_auc": float(auc),
        "confusion_matrix": cm.tolist(),
        "best_params": search.best_params_,
        "classification_report": report,
        "top_features": importances.head(10).to_dict(),
    }

    metrics_path = os.path.join(MODEL_DIR, "rf_metrics_best.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=4)

    print(f"[INFO] Metrics saved: {metrics_path}")
    print("=== TRAINING COMPLETED ===")

if __name__ == "__main__":
    train_best_rf()
