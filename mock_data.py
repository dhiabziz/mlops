import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import time

# 1. Konfigurasi Koneksi ke Postgres (via Port Forwarding localhost)
# Format: postgresql://user:password@host:port/database_name
DB_URI = "postgresql://postgres:password123@localhost:5434/mlflow_db"

def generate_and_upload():
    print("🚀 Sedang generate data Crypto palsu...")
    
    # 2. Bikin Data Dummy (Tanggal & Harga)
    dates = pd.date_range(start="2024-01-01", periods=1000, freq="H")
    # Bikin harga yang agak random tapi ada tren naik
    prices = [40000]
    for i in range(1, 1000):
        change = np.random.uniform(-50, 55) # Random naik/turun
        prices.append(prices[-1] + change)
        
    df = pd.DataFrame({
        "timestamp": dates,
        "price": prices,
        "volume": np.random.randint(100, 1000, size=1000)
    })
    
    print(f"📊 Contoh Data:\n{df.head()}")

    # 3. Upload ke Postgres
    engine = create_engine(DB_URI)
    try:
        # Simpan ke tabel bernama 'crypto_prices'
        df.to_sql('crypto_prices', engine, if_exists='replace', index=False)
        print("✅ SUKSES: Data berhasil dimasukkan ke PostgreSQL!")
    except Exception as e:
        print(f"❌ ERROR: Gagal connect ke DB. Pastikan port-forward 5434 jalan.\nError: {e}")

if __name__ == "__main__":
    generate_and_upload()