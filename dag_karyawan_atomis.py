from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Mengimpor fungsi-fungsi dari skrip ETL yang telah Anda bedah
# (Ini bisa dilakukan karena etl_pandas.py dan dag ini berada di folder yang sama)
from etl_pandas import extract_to_datalake, transform_pandas, load_db

# Aturan Main (SLA dan Retry)
aturan_default = {
    'owner': 'data_engineer',
    'retries': 1, # Jika gagal, coba lagi 1 kali
    'retry_delay': timedelta(minutes=2), # Tunggu 2 menit sebelum mencoba lagi agar tidak spam API/DB
}

# Mendefinisikan DAG (Rantai Pekerjaan)
with DAG(
    dag_id='pipeline_karyawan_atomis', 
    default_args=aturan_default,
    description='Orkestrasi ETL Karyawan dengan XCom dan Isolasi Task',
    schedule='0 2 * * *', # Run setiap jam 02.00
    start_date=datetime(2026, 5, 13),
    catchup=False,
    tags=['produksi', 'hrd', 'v2'],
) as dag:

    # Task 1: Murni Ekstraksi & Lempar ke OCI
    tugas_ekstrak = PythonOperator(
        task_id='task_extract',
        python_callable=extract_to_datalake,
    )

    # Task 2: Tarik dari OCI & Bersihkan
    tugas_transform = PythonOperator(
        task_id='task_transform',
        python_callable=transform_pandas,
    )

    # Task 3: Injeksi ke PostgreSQL
    tugas_load = PythonOperator(
        task_id='task_load',
        python_callable=load_db,
    )

    # ==========================================
    # LOGIKA DEPENDENSI (Jantung Apache Airflow)
    # ==========================================
    # Baris ini memastikan Task 2 tidak akan jalan sebelum Task 1 sukses, 
    # dan Task 3 tidak akan jalan sebelum Task 2 sukses.
    tugas_ekstrak >> tugas_transform >> tugas_load
