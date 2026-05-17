import requests
import pandas as pd
import os
from datetime import datetime

def fetch_orders():
    print("Membuka keran data: API Orders...")
    url = "http://96.9.212.102:8000/orders"

    headers = {
        "User-Agent": "MCI2026PipelineApp/1.0 (rmdhnaditya27@gmail.com)Python-Requests/2.x",
        "Accept": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()

        # Data bisa berupa list langsung atau dict dengan key tertentu
        if isinstance(data, list):
            orders = data
        elif isinstance(data, dict):
            orders = next((v for v in data.values() if isinstance(v, list)), [])
        else:
            orders = []

        if not orders:
            raise ValueError("Tidak ada data orders yang ditemukan dari API.")

        df = pd.DataFrame(orders)

        # Normalisasi nama kolom ke lowercase
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

        print(f"   Kolom tersedia: {list(df.columns)}")

        # Simpan ke Data Lake lokal
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f'/opt/airflow/data_lake/orders/orders_{current_time}.parquet'
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Gunakan engine fastparquet agar kompatibel dengan Spark
        df.to_parquet(output_path, index=False)

        print(f"✅ Sukses menyimpan {len(df)} baris ke {output_path}")

    except Exception as e:
        print(f"❌ Gagal menarik data: {e}")
        raise

if __name__ == "__main__":
    fetch_orders()