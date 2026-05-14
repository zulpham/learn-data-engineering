# (Biarkan impor, konfigurasi OCI, load_dotenv, dan fungsi get_connection() tetap seperti aslinya)

import requests
import psycopg2
import os
import logging
import json
import pandas as pd
import oci

from datetime import datetime
from dotenv import load_dotenv

# Konfigurasi Logging
logging.basicConfig(
    filename='/home/ubuntu/pipeline_pandas.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

load_dotenv('/home/ubuntu/etl_project/.env')

config_oci = oci.config.from_file()
object_storage_client = oci.object_storage.ObjectStorageClient(config_oci)
namespace_oci = object_storage_client.get_namespace().data
BUCKET_NAME = "datalake-karyawan"

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="belajar_de",
        user="postgres",
        password=os.environ.get("DB_PASS"),
        port="5432"
    )

def extract_to_datalake(**kwargs):
    """Fungsi ini hanya bertugas menarik API dan menaruhnya di OCI."""
    url = "https://jsonplaceholder.typicode.com/users"
    logging.info("[EXTRACT] Mengambil data dari API...")
    respons = requests.get(url)

    if respons.status_code == 200:
        data_json = respons.json()
        waktu_sekarang = datetime.now().strftime("%Y%m%d_%H%M%S")
        nama_file_raw = f"raw_karyawan_{waktu_sekarang}.json"
        nama_file_meta = f"raw_karyawan_{waktu_sekarang}_meta.json"

        metadata = {"source": url, "timestamp": waktu_sekarang, "record_count": len(data_json), "raw_file": nama_file_raw}

        object_storage_client.put_object(namespace_oci, BUCKET_NAME, nama_file_raw, json.dumps(data_json).encode('utf-8'))
        object_storage_client.put_object(namespace_oci, BUCKET_NAME, nama_file_meta, json.dumps(metadata).encode('utf-8'))

        logging.info(f"[DATA LAKE] Data mendarat di awan OCI: {nama_file_raw}")
        
        # HANYA KEMBALIKAN NAMA FILE (STRING), BUKAN DATA JSON
        return nama_file_raw 
    else:
        raise Exception("Gagal menarik API")

def transform_pandas(**kwargs):
    """Fungsi ini mengambil nama file dari Task sebelumnya, menariknya dari OCI, dan memprosesnya."""
    ti = kwargs['ti']
    # Tarik pesan (nama file) dari Task Extract
    nama_file_raw = ti.xcom_pull(task_ids='task_extract')
    
    if not nama_file_raw:
        raise ValueError("Gagal mendapatkan nama file mentah dari XCom!")

    logging.info(f"[TRANSFORM] Membaca {nama_file_raw} dari OCI Object Storage...")
    
    # Menarik data secara absolut dari Awan (Decoupled Storage terbukti di sini)
    objek_oci = object_storage_client.get_object(namespace_oci, BUCKET_NAME, nama_file_raw)
    data_mentah = json.loads(objek_oci.data.content.decode('utf-8'))

    df = pd.json_normalize(data_mentah)
    df.dropna(subset=['id', 'name'], inplace=True)
    df.fillna({'address.city':'Data Hilang'}, inplace=True) if 'address.city' in df.columns else df.assign(**{'address.city': 'Data Hilang'})
    df['gaji_pokok'] = 5000000 + (df['id'] * 100000)

    # Pemodelan Skema Bintang
    dim_karyawan = df[['id', 'name']].copy()
    data_dim_karyawan = list(dim_karyawan.itertuples(index=False, name=None))

    kota_unik = df['address.city'].unique()
    dim_lokasi = pd.DataFrame(kota_unik, columns=['kota'])
    dim_lokasi.index += 1
    dim_lokasi.reset_index(inplace=True)
    dim_lokasi.rename(columns={'index': 'id_lokasi'}, inplace=True)
    data_dim_lokasi = list(dim_lokasi.itertuples(index=False, name=None))

    df_fakta = df.merge(dim_lokasi, left_on='address.city', right_on='kota')[['id', 'id_lokasi', 'gaji_pokok']]
    data_fakta_gaji = list(df_fakta.itertuples(index=False, name=None))

    logging.info("[TRANSFORM] Skema Bintang berhasil dibentuk.")
    
    # Kembalikan sebagai Dictionary agar aman melintasi XCom
    return {
        "karyawan": data_dim_karyawan,
        "lokasi": data_dim_lokasi,
        "fakta": data_fakta_gaji
    }

def load_db(**kwargs):
    """Fungsi ini mengambil data bersih dari Task sebelumnya dan melakukan injeksi ke DB."""
    ti = kwargs['ti']
    # Tarik paket data bersih dari Task Transform
    paket_data = ti.xcom_pull(task_ids='task_transform')

    if not paket_data:
        raise ValueError("Gagal mendapatkan data bersih dari XCom!")

    # Koneksi harus dibuka DI DALAM task ini, tidak bisa di-passing lewat XCom
    koneksi = get_connection()
    try:
        kursor = koneksi.cursor()
        logging.info("[LOAD] Memulai eksekusi UPSERT ke Database...")

        kursor.executemany("""
            INSERT INTO dim_karyawan (id_karyawan, nama) VALUES (%s, %s)
            ON CONFLICT (id_karyawan) DO UPDATE SET nama = EXCLUDED.nama
        """, paket_data["karyawan"])

        kursor.executemany("""
            INSERT INTO dim_lokasi (id_kota, kota) VALUES (%s, %s)
            ON CONFLICT (id_kota) DO UPDATE SET kota = EXCLUDED.kota
        """, paket_data["lokasi"])

        kursor.executemany("""
            INSERT INTO fact_gaji (id_karyawan, id_kota, gaji) VALUES (%s, %s, %s)
            ON CONFLICT (id_karyawan, id_kota) DO UPDATE SET gaji = EXCLUDED.gaji
        """, paket_data["fakta"])

        koneksi.commit()
        kursor.close()
        logging.info("=== PIPELINE ATOMIS SUKSES ===")
    finally:
        koneksi.close()
