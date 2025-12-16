# import pandas as pd
# import numpy as np
# from sklearn.model_selection import train_test_split
# from xgboost import XGBClassifier
# from sklearn.metrics import classification_report, accuracy_score, confusion_matrix
# import joblib
# import os
# import json
# from datetime import datetime
# import sys

# # --- KONFIGURASI PATH ---
# if os.path.exists('/opt/airflow/dags/scripts/ml'):
#     base_dir = '/opt/airflow/dags/scripts/ml'
# else:
#     base_dir = './scripts/ml'

# INPUT_FILE = os.path.join(base_dir, 'training_data_warranty.csv')
# MODEL_DIR = os.path.join(base_dir, 'models')
# os.makedirs(MODEL_DIR, exist_ok=True)

# def train_xgb_model():
#     print("=== MEMULAI TRAINING MODEL (XGBOOST) ===")
    
#     # 1. LOAD DATA
#     try:
#         df = pd.read_csv(INPUT_FILE)
#         print(f"--> Data dimuat: {len(df)} baris.")
#     except FileNotFoundError:
#         print(f"[ERROR] File {INPUT_FILE} tidak ditemukan.")
#         sys.exit(1)

#     # 2. PREPROCESSING
#     columns_to_drop = ['is_claim', 'sales_key', 'product_name', 'quantity', 'unit_price', 'total_amount']
#     existing_cols = [c for c in columns_to_drop if c in df.columns]
    
#     X = df.drop(columns=existing_cols)
#     y = df['is_claim']
    
#     print("--> Melakukan Encoding Data Kategorikal...")
#     X = pd.get_dummies(X, drop_first=True)
    
#     # 3. HITUNG SCALE POS WEIGHT (Pengganti class_weight='balanced')
#     count_neg = np.sum(y == 0)
#     count_pos = np.sum(y == 1)
#     scale_weight = count_neg / count_pos
#     print(f"--> Ratio Data Imbalance: {scale_weight:.2f} (Negatif/Positif)")
    
#     # 4. SPLIT DATA
#     print("--> Membagi Data Training & Testing (80/20)...")
#     X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
#     # 5. TRAINING
#     print(f"--> Melatih XGBoost dengan scale_pos_weight={scale_weight:.2f}...")
#     model = XGBClassifier(
#         n_estimators=100,
#         learning_rate=0.1,
#         max_depth=5,
#         scale_pos_weight=scale_weight, # KUNCI untuk data tidak seimbang
#         random_state=42,
#         n_jobs=-1
#     )
#     model.fit(X_train, y_train)
    
#     # 6. EVALUASI
#     print("--> Evaluasi Model...")
#     y_pred = model.predict(X_test)
    
#     acc = accuracy_score(y_test, y_pred)
#     report_dict = classification_report(y_test, y_pred, output_dict=True)
#     cm = confusion_matrix(y_test, y_pred)
    
#     print(f"    Akurasi: {acc:.2%}")
#     print("\nClassification Report:")
#     print(classification_report(y_test, y_pred))
    
#     # 7. FEATURE IMPORTANCE
#     print("\n--> Faktor Penentu Risiko Terbesar (Top 5):")
#     importances = pd.Series(model.feature_importances_, index=X.columns)
#     print(importances.nlargest(5))

#     # 8. SIMPAN HASIL (CHAMPION MODEL FORMAT)
#     metrics_data = {
#         "model_type": "XGBoost",
#         "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#         "performance": {
#             "accuracy": acc,
#             "precision_class_1": report_dict['1']['precision'],
#             "recall_class_1": report_dict['1']['recall'],
#             "f1_score_class_1": report_dict['1']['f1-score'],
#             "support_class_1": report_dict['1']['support']
#         },
#         "confusion_matrix": cm.tolist(),
#         "hyperparameters": model.get_params()
#     }
    
#     metrics_path = os.path.join(MODEL_DIR, 'metrics_active.json')
#     with open(metrics_path, 'w') as f:
#         json.dump(metrics_data, f, indent=4)
        
#     model_path = os.path.join(MODEL_DIR, 'model_active.pkl')
#     joblib.dump(model, model_path)
    
#     print(f"\n--> [XGB] Model AKTIF diperbarui: {model_path}")
#     print("=== TRAINING SELESAI ===")

# if __name__ == "__main__":
#     train_xgb_model()

# import pandas as pd
# import numpy as np
# from sklearn.model_selection import train_test_split
# from xgboost import XGBClassifier
# from sklearn.metrics import classification_report, accuracy_score, confusion_matrix
# import joblib
# import os
# import json
# from datetime import datetime
# import sys

# # --- KONFIGURASI PATH ---
# if os.path.exists('/opt/airflow/dags/scripts/ml'):
#     base_dir = '/opt/airflow/dags/scripts/ml'
# else:
#     base_dir = './scripts/ml'

# INPUT_FILE = os.path.join(base_dir, 'training_data_warranty.csv')
# MODEL_DIR = os.path.join(base_dir, 'models')
# os.makedirs(MODEL_DIR, exist_ok=True)

# def train_xgb_model():
#     print("=== MEMULAI TRAINING MODEL (XGBOOST - BALANCED DATA) ===")
    
#     # 1. LOAD DATA
#     try:
#         df = pd.read_csv(INPUT_FILE)
#         print(f"--> Data dimuat: {len(df)} baris.")
#     except FileNotFoundError:
#         print(f"[ERROR] File {INPUT_FILE} tidak ditemukan.")
#         sys.exit(1)

#     # 2. PREPROCESSING
#     columns_to_drop = ['is_claim', 'sales_key', 'product_name', 'quantity', 'unit_price', 'total_amount']
#     existing_cols = [c for c in columns_to_drop if c in df.columns]
    
#     X = df.drop(columns=existing_cols)
#     y = df['is_claim']
    
#     print("--> Melakukan Encoding Data Kategorikal...")
#     X = pd.get_dummies(X, drop_first=True)
    
#     # 3. SPLIT DATA
#     print("--> Membagi Data Training & Testing (80/20)...")
#     X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
#     # 4. TRAINING
#     # PERUBAHAN: Kita HAPUS 'scale_pos_weight' karena data sudah seimbang (Undersampling)
#     print(f"--> Melatih XGBoost...")
#     model = XGBClassifier(
#         n_estimators=100,
#         learning_rate=0.1,
#         max_depth=5,
#         random_state=42,
#         n_jobs=-1
#     )
#     model.fit(X_train, y_train)
    
#     # 5. EVALUASI
#     print("--> Evaluasi Model...")
#     y_pred = model.predict(X_test)
    
#     acc = accuracy_score(y_test, y_pred)
#     report_dict = classification_report(y_test, y_pred, output_dict=True)
#     cm = confusion_matrix(y_test, y_pred)
    
#     print(f"    Akurasi: {acc:.2%}")
#     print("\nClassification Report:")
#     print(classification_report(y_test, y_pred))
    
#     # 6. FEATURE IMPORTANCE
#     print("\n--> Faktor Penentu Risiko Terbesar (Top 5):")
#     importances = pd.Series(model.feature_importances_, index=X.columns)
#     print(importances.nlargest(5))

#     # 7. SIMPAN HASIL
#     metrics_data = {
#         "model_type": "XGBoost",
#         "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#         "performance": {
#             "accuracy": acc,
#             "precision_class_1": report_dict['1']['precision'],
#             "recall_class_1": report_dict['1']['recall'],
#             "f1_score_class_1": report_dict['1']['f1-score'],
#             "support_class_1": report_dict['1']['support']
#         },
#         "confusion_matrix": cm.tolist(),
#         "hyperparameters": model.get_params()
#     }
    
#     metrics_path = os.path.join(MODEL_DIR, 'metrics_active.json')
#     with open(metrics_path, 'w') as f:
#         json.dump(metrics_data, f, indent=4)
        
#     model_path = os.path.join(MODEL_DIR, 'model_active.pkl')
#     joblib.dump(model, model_path)
    
#     print(f"\n--> [XGB] Model AKTIF diperbarui: {model_path}")
#     print("=== TRAINING SELESAI ===")

# if __name__ == "__main__":
#     train_xgb_model()

