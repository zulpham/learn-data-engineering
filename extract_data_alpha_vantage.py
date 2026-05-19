import json
import logging
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

# Konfigurasi logging Airflow sudah otomatis, tapi kita bisa pakai logging.getLogger
logger = logging.getLogger(__name__)

def upload_alpha_vantage_json():
    try:
        # 1. EKSTRAKSI VIA HTTP HOOK
        http_hook = HttpHook(http_conn_id='api_alpha_vantage', method='GET')
        koneksi_metadata = http_hook.get_connection('api_alpha_vantage')
        api_key = koneksi_metadata.password

        endpoint = f"query?function=TIME_SERIES_DAILY&symbol=QCOM&apikey={api_key}"
        logger.info("Mengeksekusi penarikan data via HttpHook...")

        response = http_hook.run(endpoint)
        response.raise_for_status()  # lempar exception jika status bukan 200
        logger.info("Data dari AlphaVantage berhasil ditarik")
        data = response.json()

        # Konversi objek JSON menjadi string teks
        string_data = json.dumps(data)

        # 2. INJEKSI STATELESS VIA WASB HOOK
        azure_hook = WasbHook(wasb_conn_id='azure_blob_bronze')
        container_name = "bronze"
        blob_name = "alpha_vantage_qcom.json"

        logger.info("Menyuntikkan data string langsung ke ADLS via WasbHook...")
        azure_hook.load_string(
            string_data=string_data,
            container_name=container_name,
            blob_name=blob_name,
            overwrite=True
        )

        logger.info("[SUKSES] File JSON telah berhasil disimpan di container 'bronze'")

    except Exception as e:
        # Menangkap pesan error
        logger.error("Pipeline gagal dijalankan: %s", str(e), exc_info=True)
        # Agar Airflow menandai sebagai kegagalan
        raise
