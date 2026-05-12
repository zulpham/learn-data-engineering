import requests
import psycopg2
import os
from dotenv import load_dotenv

# Rute absolut agar Cron tidak buta
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
    print("[EXTRACT] Menghubungi Server HRD via API...")
    url = "https://jsonplaceholder.typicode.com/users"
    respons = requests.get(url)

    # Validasi keamanan: Pastikan internet merespons dengan kode 200 (OK)
    if respons.status_code == 200:
        return respons.json()
    else:
        raise Exception(f"Gagal menarik API. Kode Status: {respons.status_code}")

def transform_api(data_json):
    """Membedah struktur JSON dan membersihkan datanya."""
    print("[TRANSFORM] Membedah struktur JSON bersarang...")
    data_bersih = []
    for item in data_json:
        id_karyawan = item['id']
        nama = item['name']

        # JSON sering kali bersarang (nested). Kita gali kotanya:
        kota = item['address']['city']

        # Simulasi perhitungan: Karena API ini tidak punya data gaji, 
        # kita ciptakan aturan bisnis (Rp 5 Juta + (ID * 100 ribu))
        gaji_pokok = 5000000 + (id_karyawan * 100000)

        data_bersih.append((id_karyawan, nama, kota, gaji_pokok))
    return data_bersih

def load_db(koneksi, data_bersih):
    """Memasukkan data ke tabel baru di database."""
    print("[LOAD] Membuat tabel dan memuat data karyawan eksternal...")
    kursor = koneksi.cursor()

    # DDL Otomatis: Buat tabel jika bos belum menyiapkannya
    kursor.execute("""
        CREATE TABLE IF NOT EXISTS karyawan_eksternal (
            id INT PRIMARY KEY,
            nama VARCHAR(150),
            kota VARCHAR(100),
            gaji INT
        )
    """)

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
    kursor.executemany(query, data_bersih)
    koneksi.commit()
    kursor.close()
    print(f"Sukses menginjeksi {len(data_bersih)} baris data.")

def main():
    koneksi = None
    try:
        koneksi = get_connection()
        data_mentah = extract_api()
        data_siap = transform_api(data_mentah)
        load_db(koneksi, data_siap)
        print("--- PIPELINE API BERHASIL ---")
    except Exception as e:
        print(f"!!! BENCANA PIPELINE: {e}")
    finally:
        if koneksi:
            koneksi.close()

if __name__ == "__main__":
    main()
