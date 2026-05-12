import requests
import psycopg2
import os
import logging
import json
import pandas as pd

from datetime import datetime
from dotenv import load_dotenv

# Konfigurasi Logging
logging.basicConfig(
    filename='/home/ubuntu/pipeline_pandas.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

load_dotenv('/home/ubuntu/.env')

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="belajar_de",
        user="postgres",
        password=os.environ.get("DB_PASS"),
        port="5432"
    )

def extract_to_datalake():
    """Menarik dari API dan langsung membuangnya ke Data Lake sebagai arsip mati."""
    url = "https://jsonplaceholder.typicode.com/users"
    logging.info("[EXTRACT] Mengambil data dari API...")
    respons = requests.get(url)

    if respons.status_code == 200:
        data_json = respons.json()

        # Membuat stempel waktu untuk nama file arsip
        waktu_sekarang = datetime.now().strftime("%Y%m%d_%H%M%S")
        nama_file_raw = f"/home/ubuntu/data_lake/raw_karyawan_{waktu_sekarang}.json"
        nama_file_meta = f"/home/ubuntu/data_lake/raw_karyawan_{waktu_sekarang}_meta.json"

        # Menyimpan data mentah ke Disk (Data Lake Lokal)
        with open(nama_file_raw, 'w') as file:
            json.dump(data_json, file)

        # Buat metadata untuk audit
        metadata = {
            "source": url,
            "timestamp": waktu_sekarang,
            "record_count": len(data_json),
            "raw_file": nama_file_raw
        }

	# Menyimpan metadata dari data mentah yang disimpan
        with open(nama_file_meta,'w') as file:
            json.dump(metadata,file)

        logging.info(f"[DATA LAKE] data mentah tersimpan di: {nama_file_raw}")
        logging.info(f"[DATA LAKE] metadata tersimpan di: {nama_file_meta}")

        return nama_file_raw

    else:
        logging.error(f"Koneksi API gagal. Status: {respons.status_code}")
        raise Exception("Gagal menarik API")

def transform_pandas(nama_file_raw):
    logging.info("[TRANSFORM] Membedah Flat Table menjadi Skema Bintang...")

    with open(nama_file_raw, 'r') as file:
        data_mentah = json.load(file)

    df = pd.json_normalize(data_mentah)
    df.dropna(subset=['id', 'name'], inplace=True)

    if 'address.city' not in df.columns:
        df['address.city'] = 'Data Hilang'
    else:
        df.fillna({'address.city':'Data Hilang'}, inplace=True)

    df['gaji_pokok'] = 5000000 + (df['id'] * 100000)

    # ==========================================
    # PEMODELAN DIMENSIONAL (STAR SCHEMA)
    # ==========================================

    # 1. DIMENSI KARYAWAN
    dim_karyawan = df[['id', 'name']].copy()
    data_dim_karyawan = list(dim_karyawan.itertuples(index=False, name=None))

    # 2. DIMENSI LOKASI
    # Ekstrak kota unik menggunakan .unique() seperti jawabanmu
    kota_unik = df['address.city'].unique()
    dim_lokasi = pd.DataFrame(kota_unik, columns=['kota'])

    # Ciptakan ID Lokasi buatan mulai dari angka 1
    dim_lokasi.index += 1 
    dim_lokasi.reset_index(inplace=True)
    dim_lokasi.rename(columns={'index': 'id_lokasi'}, inplace=True)

    data_dim_lokasi = list(dim_lokasi.itertuples(index=False, name=None))

    # 3. FAKTA GAJI
    # Gabungkan (JOIN) dataframe asli dengan dim_lokasi agar kita mendapatkan id_lokasi
    df_fakta = df.merge(dim_lokasi, left_on='address.city', right_on='kota')

    # Buang teks deskriptif! Tabel fakta hanya boleh berisi Angka dan ID
    df_fakta = df_fakta[['id', 'id_lokasi', 'gaji_pokok']]
    data_fakta_gaji = list(df_fakta.itertuples(index=False, name=None))

    logging.info("[TRANSFORM] Skema Bintang berhasil dibentuk.")

    # Mengembalikan 3 paket data terpisah
    return data_dim_karyawan, data_dim_lokasi, data_fakta_gaji

def load_db(koneksi, data_karyawan, data_lokasi, data_fakta):
    """Memuat data ke PostgreSQL."""
    logging.info("[LOAD] Memasukkan data terproses ke Database...")
    kursor = koneksi.cursor()

    # ===== Membuat/mengecek table ====
    # Cek apakah tabel 'dim_karyawan' sudah ada
    kursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'dim_karyawan'
        );
    """)
    is_tabel_ada=kursor.fetchone()[0]

    kursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_karyawan (
            id_karyawan INT PRIMARY KEY,
            nama VARCHAR(150)
        );
    """)

    if not is_tabel_ada:
        logging.info("Berhasil membuat tabel 'dim_karyawan'")


    # Cek apakah tabel 'dim_lokasi' sudah ada
    kursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'dim_lokasi'
        );
    """)

    is_tabel_ada=kursor.fetchone()[0]

    kursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_lokasi (
            id_kota INT PRIMARY KEY,
            kota VARCHAR(100)
        );
    """)

    if not is_tabel_ada:
        logging.info("Berhasil membuat tabel 'dim_lokasi'")


    # Cek apakah tabel 'fact_gaji' sudah ada
    kursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'fact_gaji'
        );
    """)

    is_tabel_ada=kursor.fetchone()[0]

    kursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_gaji (
            id_karyawan INT NOT NULL,
            id_kota INT NOT NULL,
            gaji NUMERIC(12,2) NOT NULL,
            CONSTRAINT fk_karyawan FOREIGN KEY (id_karyawan)
                REFERENCES dim_karyawan (id_karyawan),
            CONSTRAINT fk_kota FOREIGN KEY (id_kota)
                REFERENCES dim_lokasi (id_kota),
            CONSTRAINT unique_karyawan_kota UNIQUE (id_karyawan, id_kota)
        );
    """)

    if not is_tabel_ada:
        logging.info("Berhasil membuat tabel 'fact_gaji'")

    # ==== Melakukan Insert Data ke Masing-Masing Tabel ====
    # insert data ke tabel 'dim_karyawan'
    query_karyawan = """
        INSERT INTO dim_karyawan (id_karyawan, nama)
        VALUES (%s, %s)
        ON CONFLICT (id_karyawan)
        DO UPDATE SET
            nama = EXCLUDED.nama
    """
    kursor.executemany(query_karyawan, data_karyawan)
    koneksi.commit()

    logging.info("Berhasil melakukan insert data pada tabel 'dim_karyawan'")


    # insert data ke tabel 'dim_lokasi'
    query_lokasi = """
        INSERT INTO dim_lokasi (id_kota, kota)
        VALUES (%s, %s)
        ON CONFLICT (id_kota)
        DO UPDATE SET
            kota = EXCLUDED.kota
    """
    kursor.executemany(query_lokasi, data_lokasi)
    koneksi.commit()

    logging.info("Berhasil melakukan insert data pada tabel 'dim_lokasi'")


    # insert data ke tabel 'fact_gaji'
    query_fact = """
        INSERT INTO fact_gaji (id_karyawan, id_kota, gaji)
        VALUES (%s, %s, %s)
        ON CONFLICT (id_karyawan, id_kota)
        DO UPDATE SET gaji = EXCLUDED.gaji
    """
    kursor.executemany(query_fact, data_fakta)
    koneksi.commit()

    logging.info("Berhasil melakukan insert data pada tabel 'fact_gaji'")

    kursor.close()

def main():
    koneksi = None
    try:
        logging.info("=== SIKLUS ETL DIMULAI ===")

        # Pipa Data Baru
        file_arsip = extract_to_datalake()

        # Di dalam fungsi main():
        data_karyawan, data_lokasi, data_fakta = transform_pandas(file_arsip)
        
        koneksi = get_connection()
        load_db(koneksi, data_karyawan, data_lokasi, data_fakta)

        logging.info("=== PIPELINE SUKSES ===")
    except Exception as e:
        logging.error(f"BENCANA PIPELINE: {e}")
    finally:
        if koneksi:
            koneksi.close()
            logging.info("Koneksi ditutup.\n")

if __name__ == "__main__":
    main()
