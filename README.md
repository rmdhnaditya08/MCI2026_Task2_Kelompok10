# 🛒 Orders Realtime Analytics Pipeline
### MCI2026 — Task 2 | Pipeline Orchestration & Data Visualization

---

## 📖 Deskripsi Proyek

Pipeline ini membangun sistem analitik **micro-batching** untuk dataset Orders dari REST API. Data ditarik secara periodik setiap 10 menit menggunakan Apache Airflow, diproses dengan Apache Spark, dimuat ke ClickHouse sebagai Data Warehouse, lalu divisualisasikan melalui dashboard Metabase.

### Alur Sistem

```
┌──────────────────┐     ┌─────────────────┐     ┌────────────────┐     ┌──────────────┐     ┌───────────────┐
│   REST API        │────▶│   Data Lake     │────▶│ Apache Spark   │────▶│  ClickHouse  │────▶│   Metabase    │
│ /orders endpoint  │     │ (Parquet files) │     │ (Transformasi) │     │  (Warehouse) │     │  (Dashboard)  │
└──────────────────┘     └─────────────────┘     └────────────────┘     └──────────────┘     └───────────────┘
         ▲
         │  Orkestrasi setiap 10 menit
┌──────────────────┐
│  Apache Airflow  │
│  (Scheduler/DAG) │
└──────────────────┘
```

---

## 🏗️ Tech Stack

| Komponen | Teknologi | 
|---|---|
| Orchestration | Apache Airflow 2.9.x | 
| Ingestion | Python + Requests | 
| Data Lake | Apache Parquet | 
| Processing | Apache Spark (PySpark) 3.5.x | 
| Warehouse | ClickHouse | 
| Visualization | Metabase | 
| Infrastructure | Docker + Docker Compose | 

---

## 📂 Struktur Repository

```
MCI2026_Task2/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── dags/
│   ├── orders_pipeline_dag.py          # Airflow DAG utama
│   └── scripts/
│       ├── fetch_orders_stream.py      # Task 1: Ingestion API → Parquet
│       └── process_orders_spark.py     # Task 2: Spark → ClickHouse
├── data_lake/
│   └── orders/                         # Folder Parquet sementara
└── README.md
```

---

## 🔄 Penjelasan Pipeline & Kode

Pipeline terdiri dari **2 task** yang berjalan berurutan di dalam Airflow DAG.

---

### 📄 File 1: `orders_pipeline_dag.py` — Airflow DAG

File ini adalah **jantung orkestrasi** pipeline. Mendefinisikan kapan dan bagaimana setiap task dijalankan.

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mci2026_engineer',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'orders_realtime_pipeline',
    default_args=default_args,
    schedule_interval='*/10 * * * *',  # Setiap 10 menit
    catchup=False,
    max_active_runs=1,
    description='Micro-batching Orders API -> Spark -> ClickHouse'
) as dag:

    ingest_stream = BashOperator(
        task_id='fetch_orders_api',
        bash_command='python /opt/airflow/dags/scripts/fetch_orders_stream.py'
    )

    process_analytics = BashOperator(
        task_id='spark_process_and_load_clickhouse',
        bash_command='python /opt/airflow/dags/scripts/process_orders_spark.py'
    )

    ingest_stream >> process_analytics
```

**Penjelasan komponen DAG:**

| Komponen | Nilai | Keterangan |
|---|---|---|
| `dag_id` | `orders_realtime_pipeline` | Nama unik DAG di Airflow |
| `schedule_interval` | `*/10 * * * *` | Cron: jalankan setiap 10 menit |
| `catchup=False` | - | Tidak menjalankan ulang jadwal yang terlewat |
| `max_active_runs=1` | - | Hanya 1 pipeline aktif sekaligus (cegah race condition) |
| `retries=1` | - | Coba ulang 1x jika gagal |
| `>>` | - | Operator dependensi: fetch harus selesai sebelum process mulai |

**Alur task:**
```
[fetch_orders_api] ──▶ [spark_process_and_load_clickhouse]
```

---

### 📄 File 2: `fetch_orders_stream.py` — Task Ingestion

File ini menjalankan **Task 1**: menarik data dari REST API dan menyimpannya ke Data Lake dalam format Parquet.

```python
import requests
import pandas as pd
import os
from datetime import datetime

def fetch_orders():
    url = "http://96.9.212.102:8000/orders"
    headers = {
        "User-Agent": "MCI2026PipelineApp/1.0 Python-Requests/2.x",
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers, timeout=15)
    data = response.json()

    # Tangani berbagai format response (list atau dict)
    if isinstance(data, list):
        orders = data
    elif isinstance(data, dict):
        orders = next((v for v in data.values() if isinstance(v, list)), [])

    df = pd.DataFrame(orders)

    # Normalisasi nama kolom ke lowercase
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # Simpan ke Data Lake dengan nama file berbasis timestamp
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f'/opt/airflow/data_lake/orders/orders_{current_time}.parquet'
    df.to_parquet(output_path, index=False)
```

**Alur kerja fetch:**

```
GET http://96.9.212.102:8000/orders
        │
        ▼
Parse JSON → DataFrame pandas
        │
        ▼
Normalisasi kolom (lowercase, strip spasi)
        │
        ▼
Simpan Parquet → /opt/airflow/data_lake/orders/orders_YYYYMMDD_HHMMSS.parquet
```

**Struktur data yang diterima dari API:**

| Kolom | Tipe | Keterangan |
|---|---|---|
| `order_id` | integer | ID unik order |
| `user_id` | integer | ID pengguna |
| `order_number` | integer | Urutan order ke-N dari user |
| `order_dow` | integer | Hari dalam seminggu (0=Sunday s.d. 6=Saturday) |
| `order_hour_of_day` | integer | Jam pemesanan (0-23) |
| `days_since_prior_order` | float | Jarak hari dari order sebelumnya |
| `eval_set` | string | Set evaluasi (prior/train/test) |
| `products` | array | List produk dalam order — **data nested** |

**Struktur nested `products` (setiap elemen array):**

```json
{
  "add_to_cart_order": 1,
  "aisle": "ice cream ice",
  "aisle_id": 37,
  "department": "frozen",
  "department_id": 1,
  "product_id": 38563,
  "product_name": "Mint Chocolate Chip Ice Cream",
  "reordered": 1
}
```

> **Catatan penting:** Kolom `products` berisi array of struct (nested). Inilah yang membuat proses di Spark harus melakukan `explode` terlebih dahulu sebelum bisa diagregasi.

---

### 📄 File 3: `process_orders_spark.py` — Task Processing & Load

File ini menjalankan **Task 2**: membaca Parquet dari Data Lake, melakukan 5 agregasi dengan Spark, lalu memuat hasilnya ke 5 tabel ClickHouse.

#### Bagian 1 — Inisialisasi Spark & Baca Data

```python
spark = SparkSession.builder \
    .appName("Orders_Streaming_Analytics") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

# Baca SEMUA file Parquet di folder sekaligus
df_raw = spark.read.parquet("file:///opt/airflow/data_lake/orders/")
```

#### Bagian 2 — Explode & Flatten Array Products

Ini adalah langkah paling krusial. Array `products` harus dipecah menjadi baris individual.

```python
# Explode: 1 baris order → N baris (sebanyak produk dalam order)
df_exploded = df_raw.withColumn("product", F.explode(F.col("products")))

# Flatten: ambil field dari dalam struct
df_flat = df_exploded.select(
    F.col("order_id"),
    F.col("order_dow"),
    F.col("order_hour_of_day"),
    F.col("product.product_name").alias("product_name"),
    F.col("product.department").alias("department"),
    F.col("product.aisle").alias("aisle"),
    F.col("product.reordered").alias("reordered"),
)
```

**Ringkasan 5 tabel output:**

| # | Tabel ClickHouse | Sumber | Metrik Utama |
|---|---|---|---|
| 1 | `orders_department_summary` | `df_flat` | total_items_ordered per department |
| 2 | `orders_top_products` | `df_flat` | total_ordered per produk |
| 3 | `orders_top_aisle` | `df_flat` | total_items_ordered per aisle |
| 4 | `orders_by_dow` | `df_raw` | total_orders per hari |
| 5 | `orders_by_hour` | `df_raw` | total_orders per jam |

---

