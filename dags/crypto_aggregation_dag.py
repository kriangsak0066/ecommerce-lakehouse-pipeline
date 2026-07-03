from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import duckdb
import pandas as pd

def run_lakehouse_analytics():
    print("🚀 Starting Data Lakehouse Analytics using DuckDB...")
    
    # เชื่อมต่อ DuckDB และตั้งค่าเชื่อมต่อกับ MinIO (S3)
    conn = duckdb.connect()
    
    # โหลด extension สำหรับอ่าน AWS S3
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    
    # ตั้งค่า Secret สำหรับเข้า MinIO
    conn.execute("""
        CREATE SECRET (
            TYPE S3,
            KEY_ID 'admin',
            SECRET 'password',
            REGION 'us-east-1',
            ENDPOINT 'minio:9000',
            URL_STYLE 'path',
            USE_SSL false
        );
    """)
    
    # เขียน SQL Query เพื่ออ่านข้อมูลจาก Delta Lake โดยตรง!
    # นี่คือพลังของ Data Lakehouse ที่ให้เราใช้ SQL คิวรีไฟล์ได้เลยโดยไม่ต้องมี Database
    query = """
        SELECT 
            symbol,
            COUNT(*) as total_trades,
            AVG(price) as avg_price,
            SUM(price * quantity) as total_volume_usd
        FROM delta_scan('s3://lakehouse/data/crypto_trades')
        GROUP BY symbol
        ORDER BY total_volume_usd DESC;
    """
    
    print("📊 Executing SQL Query on Delta Lake...")
    # fetchdf() คือการดึงผลลัพธ์จาก DuckDB ออกมาเป็น Pandas DataFrame 
    df_result = conn.execute(query).fetchdf()
    
    print("💾 Saving aggregated data to PostgreSQL (Serving Layer)...")
    # 1. สร้าง Connection ชี้ไปที่ PostgreSQL (ที่รันอยู่ใน Docker ชื่อ 'postgres')
    # รูปแบบคือ: postgresql://[user]:[password]@[host]:[port]/[database]
    engine = create_engine('postgresql://airflow:airflow_password@postgres:5432/airflow')
    
    # 2. สั่งให้ Pandas DataFrame เขียนข้อมูลลง Database 
    # - name='crypto_summary' คือชื่อตารางที่จะสร้างขึ้นมาใหม่
    # - if_exists='replace' แปลว่าถ้ามีตารางนี้อยู่แล้ว ให้ลบทิ้งแล้วเอาข้อมูลชุดใหม่ใส่แทน (เพราะเป็นตารางสรุปยอดล่าสุด)
    # - index=False แปลว่าไม่ต้องเอาเลข Row Index (0,1,2,3...) ไปใส่ใน Database
    df_result.to_sql(name='crypto_summary', con=engine, if_exists='replace', index=False)
    
    print("✅ Successfully updated 'crypto_summary' table in PostgreSQL!")
    
    return "Analytics completed successfully"
    
# กำหนดการตั้งค่าของ Airflow DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# สร้าง DAG ที่จะรันทุกๆ 5 นาที (ในชีวิตจริงอาจจะรันทุกชั่วโมง หรือทุกวัน)
with DAG(
    'crypto_lakehouse_analytics',
    default_args=default_args,
    description='A simple DuckDB analytics DAG',
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=['crypto', 'lakehouse', 'duckdb'],
) as dag:

    analytics_task = PythonOperator(
        task_id='run_duckdb_sql_aggregation',
        python_callable=run_lakehouse_analytics,
    )

    analytics_task
