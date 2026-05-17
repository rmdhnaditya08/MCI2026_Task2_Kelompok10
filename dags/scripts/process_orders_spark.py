from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from clickhouse_driver import Client
import os
import glob

def run_spark_analytics():
    spark = SparkSession.builder \
        .appName("Orders_Streaming_Analytics") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    print("Membaca seluruh aliran data dari Data Lake...")
    df_raw = spark.read.parquet("file:///opt/airflow/data_lake/orders/")
    print(f"Total baris terbaca: {df_raw.count()}")

    df_exploded = df_raw.withColumn("product", F.explode(F.col("products")))

    df_flat = df_exploded.select(
        F.col("order_id"),
        F.col("user_id"),
        F.col("order_dow"),
        F.col("order_hour_of_day"),
        F.col("days_since_prior_order"),
        F.col("eval_set"),
        F.col("product.product_id").alias("product_id"),
        F.col("product.product_name").alias("product_name"),
        F.col("product.department").alias("department"),
        F.col("product.aisle").alias("aisle"),
        F.col("product.reordered").alias("reordered"),
        F.col("product.add_to_cart_order").alias("add_to_cart_order")
    )

    # Top Department (Heavy Hitters) 
    print("Kalkulasi Top Department (Heavy Hitters)...")
    department_df = df_flat.groupBy("department") \
        .agg(
            F.count("product_id").alias("total_items_ordered"),
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct("product_id").alias("unique_products"),
            F.sum("reordered").alias("total_reordered")
        ) \
        .orderBy(F.desc("total_items_ordered"))

    # Top 20 Produk Terlaris 
    print("Kalkulasi Top 20 Produk Terlaris...")
    products_df = df_flat.groupBy("product_id", "product_name", "department", "aisle") \
        .agg(
            F.count("order_id").alias("total_ordered"),
            F.sum("reordered").alias("total_reordered")
        ) \
        .orderBy(F.desc("total_ordered")) \
        .limit(20)

    # Top Aisle 
    print("Kalkulasi Top Aisle...")
    aisle_df = df_flat.groupBy("aisle", "department") \
        .agg(
            F.count("product_id").alias("total_items_ordered"),
            F.countDistinct("product_id").alias("unique_products")
        ) \
        .orderBy(F.desc("total_items_ordered")) \
        .limit(20)

    # Distribusi Order per Hari dalam Seminggu
    print("Kalkulasi Distribusi Order per Day of Week...")
    dow_df = df_raw.groupBy("order_dow") \
        .agg(
            F.count("order_id").alias("total_orders"),
            F.avg("days_since_prior_order").alias("avg_days_since_prior")
        ) \
        .orderBy("order_dow")

    # Distribusi Order per Jam 
    print("Kalkulasi Distribusi Order per Hour of Day...")
    hour_df = df_raw.groupBy("order_hour_of_day") \
        .agg(
            F.count("order_id").alias("total_orders")
        ) \
        .orderBy("order_hour_of_day")

    # Konversi ke Pandas
    department_pd = department_df.toPandas()
    products_pd   = products_df.toPandas()
    aisle_pd      = aisle_df.toPandas()
    dow_pd        = dow_df.toPandas()
    hour_pd       = hour_df.toPandas()

    spark.stop()

    print("Memuat ke ClickHouse Warehouse...")
    client = Client(
        host='clickhouse-server',
        user='admin',
        password='rahasia'
    )

    client.execute('CREATE DATABASE IF NOT EXISTS analytics')

    # Tabel 1: Department Summary 
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.orders_department_summary (
            department          String,
            total_items_ordered Int32,
            total_orders        Int32,
            unique_products     Int32,
            total_reordered     Int32
        ) ENGINE = MergeTree()
        ORDER BY total_items_ordered
    ''')
    client.execute('TRUNCATE TABLE analytics.orders_department_summary')
    if not department_pd.empty:
        rows = [tuple(r) for r in department_pd[
            ['department','total_items_ordered','total_orders','unique_products','total_reordered']
        ].itertuples(index=False, name=None)]
        client.execute('INSERT INTO analytics.orders_department_summary VALUES', rows)

    # Top 20 Produk 
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.orders_top_products (
            product_id      Int32,
            product_name    String,
            department      String,
            aisle           String,
            total_ordered   Int32,
            total_reordered Int32
        ) ENGINE = MergeTree()
        ORDER BY total_ordered
    ''')
    client.execute('TRUNCATE TABLE analytics.orders_top_products')
    if not products_pd.empty:
        rows = [tuple(r) for r in products_pd[
            ['product_id','product_name','department','aisle','total_ordered','total_reordered']
        ].itertuples(index=False, name=None)]
        client.execute('INSERT INTO analytics.orders_top_products VALUES', rows)

    # Tabel 3: Top Aisle 
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.orders_top_aisle (
            aisle               String,
            department          String,
            total_items_ordered Int32,
            unique_products     Int32
        ) ENGINE = MergeTree()
        ORDER BY total_items_ordered
    ''')
    client.execute('TRUNCATE TABLE analytics.orders_top_aisle')
    if not aisle_pd.empty:
        rows = [tuple(r) for r in aisle_pd[
            ['aisle','department','total_items_ordered','unique_products']
        ].itertuples(index=False, name=None)]
        client.execute('INSERT INTO analytics.orders_top_aisle VALUES', rows)

    # Tabel 4: Order per Day of Week 
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.orders_by_dow (
            order_dow            Int32,
            total_orders         Int32,
            avg_days_since_prior Float64
        ) ENGINE = MergeTree()
        ORDER BY order_dow
    ''')
    client.execute('TRUNCATE TABLE analytics.orders_by_dow')
    if not dow_pd.empty:
        dow_pd['avg_days_since_prior'] = dow_pd['avg_days_since_prior'].fillna(0.0)
        rows = [tuple(r) for r in dow_pd[
            ['order_dow','total_orders','avg_days_since_prior']
        ].itertuples(index=False, name=None)]
        client.execute('INSERT INTO analytics.orders_by_dow VALUES', rows)

    # Tabel 5: Order per Hour of Day 
    client.execute('''
        CREATE TABLE IF NOT EXISTS analytics.orders_by_hour (
            order_hour_of_day Int32,
            total_orders      Int32
        ) ENGINE = MergeTree()
        ORDER BY order_hour_of_day
    ''')
    client.execute('TRUNCATE TABLE analytics.orders_by_hour')
    if not hour_pd.empty:
        rows = [tuple(r) for r in hour_pd[
            ['order_hour_of_day','total_orders']
        ].itertuples(index=False, name=None)]
        client.execute('INSERT INTO analytics.orders_by_hour VALUES', rows)

    print("Membersihkan file Parquet lama dari Data Lake...")
    files = glob.glob('/opt/airflow/data_lake/orders/*.parquet')
    for f in files:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error: {f} : {e.strerror}")

    print("Pipeline Orders Selesai!")

if __name__ == "__main__":
    run_spark_analytics()