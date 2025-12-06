import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score
import joblib
import os

# --- KONFIGURASI ---
INPUT_FILE = './scripts/ml/training_data_churn.csv'
MODEL_DIR = './scripts/ml/models'

def train_churn():
    print("=== MEMULAI TRAINING MODEL (CHURN PREDICTION) ===")
    
    # 1. Load Data
    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        print("[ERROR] File tidak ditemukan.")
        return

    # 2. Preprocessing
    # Buang kolom ID dan Tanggal (karena sudah diganti Recency)
    # Target kita adalah 'is_churn'
    # PERHATIAN: Kita juga buang 'recency_days' dari fitur latih!
    # Kenapa? Karena 'is_churn' dibuat langsung dari 'recency'. Kalau tidak dibuang, model akan curang (Data Leakage).
    # Model harus memprediksi churn berdasarkan profil (umur, membership) dan pola belanja (freq, monetary), bukan recency.
    
    X = df.drop(columns=['is_churn', 'customer_key', 'last_purchase_date', 'recency_days'])
    y = df['is_churn']
    
    print("--> Fitur yang digunakan:", X.columns.tolist())
    
    # Encoding (Age Group, Membership)
    X = pd.get_dummies(X, drop_first=True)
    
    # 3. Split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # 4. Training (Random Forest)
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    # 5. Evaluasi
    y_pred = model.predict(X_test)
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred))
    
    # 6. Feature Importance
    print("\n--> Faktor Penentu Churn Terbesar:")
    imp = pd.Series(model.feature_importances_, index=X.columns)
    print(imp.nlargest(5))
    
    # 7. Simpan
    joblib.dump(model, os.path.join(MODEL_DIR, 'churn_model.pkl'))
    print("=== SELESAI ===")

if __name__ == "__main__":
    train_churn()