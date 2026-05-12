import psycopg2
import csv
from datetime import date

try:
    # EXTRACT
    conn = psycopg2.connect(
        host="localhost",
        database="belajar_de",
        user="postgres",
        password="#Admin1234",
        port="5432"
    )
    kursor = conn.cursor()
    kursor.execute("SELECT * FROM karyawan;")
    data_mentah = kursor.fetchall()

    # TRANSFORM & LOAD
    nama_file = f"transformed_gaji_karyawan_{date.today()}.csv"
    print(f"Memproses data dan memuat ke {nama_file}...")

    with open(nama_file, mode='w', newline='') as file_csv:
        writer = csv.writer(file_csv)
        # Tulis baris Header (Nama Kolom)
        writer.writerow(['ID_Karyawan', 'Nama', 'Gaji_Kotor', 'Pajak_10_Persen', 'Gaji_Bersih', 'Status_Admin'])

        # Loop Transformasi untuk setiap baris data
        for baris in data_mentah:
            id_karyawan = baris[0]
            nama = baris[1]
            gaji_kotor = baris[2]
            is_admin = baris[3]

            # Logika Transformasi Bisnis
            pajak = int(gaji_kotor * 0.10)
            gaji_bersih = gaji_kotor - pajak

            # Load (Tulis) baris yang sudah ditransformasi ke CSV
            writer.writerow([id_karyawan, nama, gaji_kotor, pajak, gaji_bersih, is_admin])

    print("PROSES ETL SELESAI. File CSV berhasil dibuat.")

except Exception as e:
    print(f"GAGAL TOTAL. Terjadi error: {e}")

finally:
    if 'kursor' in locals():
        kursor.close()
    if 'conn' in locals():
        conn.close()
