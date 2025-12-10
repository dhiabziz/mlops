import mlflow.sklearn
import pandas as pd
import os
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

# --- KONFIGURASI ENV (Sama kayak train.py) ---
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://localhost:9000"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "password123"

# Setup MLflow
mlflow.set_tracking_uri("http://localhost:8080")
EXPERIMENT_NAME = "Crypto_Price_Prediction"

app = FastAPI(title="Crypto Prediction API")

# --- 1. Fungsi Otomatis Cari Model Terbaru ---
def load_latest_model():
    print("🔍 Mencari model terbaik di MLflow...")
    experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
    if experiment is None:
        raise Exception("Eksperimen tidak ditemukan! Jalankan train.py dulu.")
    
    # Ambil run terakhir
    runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id], 
                              order_by=["start_time DESC"], 
                              max_results=1)
    
    if runs.empty:
        raise Exception("Belum ada Run/Model. Jalankan train.py dulu.")
    
    run_id = runs.iloc[0]["run_id"]
    model_uri = f"runs:/{run_id}/model_crypto_v1"
    
    print(f"📥 Loading model dari Run ID: {run_id} ...")
    model = mlflow.sklearn.load_model(model_uri)
    print("✅ Model berhasil dimuat!")
    return model

# Load model saat aplikasi start (Global Variable)
model = load_latest_model()

# --- 2. Definisi Format Input API ---
class PredictionRequest(BaseModel):
    timestamp_unix: int  # Contoh: 1704067200

# --- 3. Endpoint Prediksi ---
@app.post("/predict")
def predict_price(request: PredictionRequest):
    # Ubah input JSON jadi DataFrame (format yang dimengerti model)
    data = pd.DataFrame([[request.timestamp_unix]], columns=['timestamp_numeric'])
    
    # Prediksi
    prediction = model.predict(data)
    
    return {
        "input_timestamp": request.timestamp_unix,
        "predicted_price": float(prediction[0]),
        "status": "success"
    }

@app.get("/")
def root():
    return {"message": "Crypto MLOps API is Running!"}

if __name__ == "__main__":
    # Jalanin server di localhost port 8000
    uvicorn.run(app, host="0.0.0.0", port=8000)