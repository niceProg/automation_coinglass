Saya telah berhasil menonaktifkan service pipeline spot_aggregated_ask_bids_history. Berikut yang saya lakukan:

  1. main.py: Memberikan komentar pada baris 44, 86, 487, dan 624 untuk menandai bahwa pipeline ini dinonaktifkan
  2. coinglass_service.py: Memberikan komentar pada import (baris 52) dan definisi pipeline dictionary (baris 359-368)
  3. pipelines/init.py: Memberikan komentar pada export (baris 40 dan 81)

  Perubahan yang dibuat:

  1. coinglass_service.py:
    - Comment import: # spot_aggregated_ask_bids_history,  # DISABLED
    - Comment definisi pipeline: Seluruh block dictionary dinonaktifkan
  2. pipelines/init.py:
    - Comment export: # spot_aggregated_ask_bids_history,  # DISABLED
    - Comment all list: # "spot_aggregated_ask_bids_history",  # DISABLED
  3. main.py:
    - Comment deskripsi: # spot_aggregated_ask_bids_history [DISABLED]
    - Comment pada contoh command: # spot_aggregated_ask_bids_history [DISABLED]
    - Comment pada help text: spot_aggregated_ask_bids_history [DISABLED]