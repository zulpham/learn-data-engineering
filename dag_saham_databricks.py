from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from extract_data_alpha_vantage import upload_alpha_vantage_json
import logging

# Aturan Main (SLA dan Retry)
aturan_default = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# Fungsi simulasi untuk memastikan Airflow siap sebelum menembak Azure
def cek_kesiapan_sistem():
    logging.info("[SYSTEM] Memulai siklus orkestrasi lintas awan...")
    logging.info("[SYSTEM] Target: Azure Databricks Workspace.")

with DAG(
    dag_id='pipeline_saham_lintas_awan',
    default_args=aturan_default,
    description='Memicu Azure Databricks Job dari OCI Airflow',
    schedule='0 6 * * *', # Dijalankan setiap jam 6 pagi
    start_date=datetime(2026, 5, 16),
    catchup=False,
    tags=['produksi', 'azure', 'saham'],
) as dag:

    # Task 1: Pengecekan Sistem (Berjalan di OCI)
    tugas_pemanasan = PythonOperator(
        task_id='cek_sistem_lokal',
        python_callable=cek_kesiapan_sistem,
    )

    # Task 2: Memasukkan data json AlphaVantage terbaru ke container 'bronze'
    tugas_upload_json = PythonOperator(
        task_id='upload_json',
        python_callable=upload_alpha_vantage_json
    )

    # Task 3: Eksekusi Jarak Jauh (Menembak Azure Databricks)
    tugas_eksekusi_spark = DatabricksRunNowOperator(
        task_id='trigger_databricks_job',
        databricks_conn_id='koneksi_databricks_azure',
        job_id=61953983512862, # <--- GANTI ANGKA INI
    )

    # Logika Dependensi
    tugas_pemanasan >> tugas_upload_json >> tugas_eksekusi_spark
