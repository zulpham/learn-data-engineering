import psycopg2

try:
    # 1. Mengetuk pintu dan menyerahkan kredensial
    print("Mencoba terhubung ke database...")
    conn = psycopg2.connect(
        host="localhost",
        database="belajar_de",
        user="postgres",
        password="#Admin1234",
        port="5432"
    )

    # 2. Membuat kursor (alat bantu untuk mengeksekusi SQL di Python)
    kursor = conn.cursor()

    # 3. Mengeksekusi instruksi SQL murni
    print("Mengeksekusi query ekstraksi...")
    kursor.execute("SELECT * FROM karyawan;")

    # 4. Mengambil seluruh hasil dan memasukkannya ke memori Python
    data_mentah = kursor.fetchall()

    # 5. Memproses (mencetak) hasil ke layar
    print("\n--- HASIL EKSTRAKSI DATA KARYAWAN ---")
    for baris in data_mentah:
        print(f"ID: {baris[0]} | Nama: {baris[1]} | Gaji: Rp {baris[2]} | Admin: {baris[3]}")

except Exception as e:
    print(f"Terjadi error: {e}")

finally:
    # 6. Protokol Keamanan: Selalu tutup koneksi, apa pun yang terjadi
    if 'kursor' in locals():
        kursor.close()
    if 'conn' in locals():
        conn.close()
        print("--- KONEKSI DITUTUP DENGAN AMAN ---")
