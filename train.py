import pandas as pd
import mlflow
import numpy as np
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sqlalchemy import create_engine
import os

# --- KONFIGURASI ENVIRONMENT ---
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://localhost:9000"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "password123"

# Koneksi Database & MLflow
DB_URI = "postgresql://postgres:password123@localhost:5434/mlflow_db"
mlflow.set_tracking_uri("http://localhost:8080")
mlflow.set_experiment("Crypto_Price_Prediction")

def train_model():
    print("🔄 Mengambil data dari Postgres...")
    engine = create_engine(DB_URI)
    
    # 1. Load Data
    try:
        df = pd.read_sql("SELECT * FROM crypto_prices", engine)
        print(f"✅ Data dimuat: {len(df)} baris.")
    except Exception as e:
        print("❌ Gagal baca DB. Jalankan mock_data.py dulu!")
        return

    # 2. Preprocessing Simpel (Ubah Tanggal jadi Angka)
    df['timestamp_numeric'] = df['timestamp'].astype('int64') // 10**9
    X = df[['timestamp_numeric']]
    y = df['price']

    # Split Data (80% train, 20% test)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    print("🤖 Mulai Training Model...")
    
    # --- START MLFLOW RUN ---
    with mlflow.start_run():
        # 3. Training
        model = LinearRegression()
        model.fit(X_train, y_train)

        # 4. Evaluasi
        predictions = model.predict(X_test)
        # GANTI BARIS ERROR TADI DENGAN INI:
        mse = mean_squared_error(y_test, predictions)
        rmse = np.sqrt(mse)  # Kita akarkan manual pakai numpy
        
        print(f"📈 Model Accuracy (RMSE): {rmse}")

        # 5. Logging ke MLflow
        mlflow.log_param("model_type", "LinearRegression")
        mlflow.log_param("data_source", "Mock_Postgres")
        mlflow.log_metric("rmse", rmse)
        
        # Simpan Model ke MinIO via MLflow
        mlflow.sklearn.log_model(model, "model_crypto_v1")
        
        print("✅ SUKSES: Model tersimpan di MLflow & MinIO!")

if __name__ == "__main__":
    train_model()