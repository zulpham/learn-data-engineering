import psycopg2
import os

try:

    # EXTRACT

    koneksi = psycopg2.connect(

        host="localhost",

        database="belajar_de",

        user="postgres",

        password=os.environ.get("DB_PASS"),

        port="5432"

    )

    kursor = koneksi.cursor()

    kursor.execute("SELECT * FROM karyawan;")

    data_mentah = kursor.fetchall()


    # TRANSFORM

    # Cek apakah tabel sudah ada

    kursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'laporan_keuangan'
        )
    """)

    tabel_ada = kursor.fetchone()[0]

    # Buat tabel jika belum ada

    create_table_query = """
    CREATE TABLE IF NOT EXISTS laporan_keuangan (
        id INT PRIMARY KEY,
        pajak NUMERIC(12,2) NOT NULL,
        gaji_bersih NUMERIC(12,2) NOT NULL,
        CONSTRAINT fk_karyawan FOREIGN KEY (id)
            REFERENCES karyawan (id)
            ON DELETE CASCADE
    )
    """

    kursor.execute(create_table_query)

    koneksi.commit()

    if tabel_ada:

        print("Tabel laporan_keuangan telah diperbarui")
    else:

        print("Tabel laporan_keuangan berhasil dibuat!")

    list_data = []

    # Loop Transformasi untuk setiap baris data

    for baris in data_mentah:

        id_karyawan = baris[0]

        gaji = baris[2]


        # Logika Transformasi Bisnis

        pajak = int(gaji * 0.10)

        gaji_bersih = gaji - pajak



        # Masukkan data yang sudah di transform ke list_data

        list_data.append((id_karyawan,pajak,gaji_bersih))

    # Load

    query_upsert = """
         INSERT INTO laporan_keuangan (id, pajak, gaji_bersih)
         VALUES (%s, %s, %s)
         ON CONFLICT (id)
         DO UPDATE SET
             pajak = EXCLUDED.pajak,
             gaji_bersih = EXCLUDED.gaji_bersih
    """

    kursor.executemany(query_upsert, list_data)

    koneksi.commit()

    print("PROSES ETL SELESAI. DATA SUDAH DIPERBARUI DI POSTGRES SQL")


except Exception as e:

    print(f"GAGAL TOTAL. Terjadi error: {e}")



finally:

    if 'kursor' in locals():

        kursor.close()

    if 'koneksi' in locals():

        koneksi.close()
