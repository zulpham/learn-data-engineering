import requests # untuk berinteraksi dengan API
import psycopg2 # untuk berinteraksi dengan postgres
import os # untuk mendapatkan nilai dari os
import logging # untuk mencatat rekam jejak

from dotenv import load_dotenv # untuk berinteraksi dengan .env

# Konfigurasi logging
logging.basicConfig(
    filename='/home/ubuntu/pipeline_api.log', # file dimana log ditulis
    level=logging.INFO, # menampilkan semua pesan log(INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s' # bagaimana format pesan log dibuat - %(...)s adalah placeholder - asctime adalah timestamp - levelnames adalah level log - message adalah isi pesan log
)

# Rute absolut .env
load_dotenv('/home/ubuntu/.env')

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="belajar_de",
        user="postgres",
        password=os.environ.get("DB_PASS"),
        port="5432"
    )

def extract_api():
    """Menarik JSON mentah dari internet."""
    url = "https://jsonplaceholder.typicode.com/users"
    logging.info("Memulai koneksi ke server via API")
    respons = requests.get(url)

    # Validasi keamanan: Pastikan internet merespons dengan kode 200 (OK)
    if respons.status_code == 200:
        logging.info("Berhasil terhubung. Data berhasil ditarik dalam format JSON")
        return respons.json()
    else:
        logging.error(f"Koneksi gagal. Kode Status: {respons.status_code}")
        raise Exception(f"Gagal menarik API. Kode Status: {respons.status_code}")

def transform_api(data_json):
    """Membedah struktur JSON dan membersihkan datanya."""
    logging.info("[TRANSFORM] Membedah struktur JSON...")
    data_bersih = []
    baris_dilewati = 0

    for item in data_json:
        id_karyawan = item.get('id')
        nama = item.get('name')

	# Jika ID atau Nama tidak ada, data ini sampah. Jangan diproses!
        if id_karyawan is None or nama is None:
            baris_dilewati += 1
            continue # Langsung lompat ke karyawan berikutnya

        # Jika kota tidak ada maka kita isi dengan 'Data Hilang':
        kota = item.get('address',{}).get('city','Data Hilang')

        # Simulasi perhitungan: Karena API ini tidak punya data gaji, 
        # kita ciptakan aturan bisnis (Rp 5 Juta + (ID * 100 ribu))
        gaji_pokok = 5000000 + (id_karyawan * 100000)

        data_bersih.append((id_karyawan, nama, kota, gaji_pokok))

    logging.info(f"[TRANSFORM] Selesai. {baris_dilewati} baris data cacat dibuang.")
    return data_bersih

def load_db(koneksi, data_bersih):
    """Memasukkan data ke tabel baru di database."""
    kursor = koneksi.cursor()

    # Cek apakah tabel sudah ada
    kursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'karyawan_eksternal'
        )
    """)

    cek_tabel=kursor.fetchone()[0]

    # DDL Otomatis: Buat tabel jika belum ada
    kursor.execute("""
        CREATE TABLE IF NOT EXISTS karyawan_eksternal (
            id INT PRIMARY KEY,
            nama VARCHAR(150),
            kota VARCHAR(100),
            gaji INT
        )
    """)

    if not cek_tabel:
        logging.info("Berhasil membuat tabel 'karyawan_eksternal'")

    # UPSERT Logika
    query = """
        INSERT INTO karyawan_eksternal (id, nama, kota, gaji)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id)
        DO UPDATE SET
            nama = EXCLUDED.nama,
            kota = EXCLUDED.kota,
            gaji = EXCLUDED.gaji
    """

    logging.info("[LOAD] Memuat data karyawan...")
    kursor.executemany(query, data_bersih)
    koneksi.commit()
    kursor.close()
    logging.info(f"Sukses menginjeksi {len(data_bersih)} baris data.")

def main():
    koneksi = None
    try:
        logging.info("=== PIPELINE API DIMULAI ===")
        koneksi = get_connection()
        data_mentah = extract_api()
        data_siap = transform_api(data_mentah)
        load_db(koneksi, data_siap)
        logging.info("--- PIPELINE API BERHASIL ---")
    except Exception as e:
        logging.error(f"!!! BENCANA PIPELINE: {e}")
    finally:
        if koneksi:
            koneksi.close()
            logging.info("Koneksi database terputus dengan aman.\n")

if __name__ == "__main__":
    main()
