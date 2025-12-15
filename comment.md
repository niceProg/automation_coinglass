Pipeline spot_aggregated_ask_bids_history sudah saya nonaktifkan dengan cara:

  1. main.py:
    - Comment out di deskripsi pipeline
    - Comment out di contoh command
    - Comment out di help display
    - Comment out di documentation string
    - Comment out dari daftar default pipelines
  2. app/services/coinglass_service.py:
    - Comment out import
    - Comment out konfigurasi pipeline
  3. docker-compose.yml:
    - Comment out seluruh service definition
  4. app/providers/coinglass/pipelines/init.py:
    - Comment out import
    - Comment out dari all list