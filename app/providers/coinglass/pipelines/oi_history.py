# app/providers/coinglass/pipelines/oi_history.py
# DISABLED - Commented out as per user request
# import logging
# from typing import Any, Dict, List
# from app.repositories.coinglass_repository import CoinglassRepository
#
# logger = logging.getLogger(__name__)
#
#
# def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     OI History Pipeline
#     Cadence: 1–5m (or at least 15m)
#     Timeframes: 1m, 5m, 15m
#
#     Fetches OI OHLC history by exchange, symbol, and interval
#     """
#     repo = CoinglassRepository(conn, logger)
#
#     # OI specific timeframes (high frequency)
#     TIMEFRAMES = params.get("timeframes", ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"])
#     SYMBOLS = params.get("symbols", ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"])
#     EXCHANGES = params.get("exchanges", ["Binance", "Bybit"])
#     UNIT = params.get("unit", "usd")
#
#     summary = {
#         "oi_history": 0,
#         "oi_history_duplicates": 0,
#         "oi_history_fetches": 0
#     }
#
#     # OI OHLC History by exchange, symbol, and interval
#     for exchange in EXCHANGES:
#         for symbol in SYMBOLS:
#             pair = f"{symbol}USDT"
#             for interval in TIMEFRAMES:
#                 try:
#                     rows = client.get_oi_history(
#                         exchange=exchange, symbol=pair, interval=interval,
#                         unit=UNIT
#                     )
#                     if rows:
#                         saved = repo.upsert_oi_history(
#                             exchange=exchange, pair=pair, interval=interval,
#                             rows=rows, unit=UNIT
#                         )
#                         logger.info(
#                             f"✅ oi_history[{exchange}:{pair}:{interval}]: "
#                             f"received={len(rows)}, saved={saved}"
#                         )
#                         summary["oi_history"] += saved
#                     else:
#                         logger.info(
#                             f"⚠️ oi_history[{exchange}:{pair}:{interval}]: No data (skipped)"
#                         )
#                     summary["oi_history_fetches"] += 1
#                 except Exception as e:
#                     logger.warning(
#                         f"⚠️ oi_history[{exchange}:{pair}:{interval}]: Exception: {e} (skipped)"
#                     )
#                     summary["oi_history_fetches"] += 1
#                     continue
#
#     logger.info(
#         f"📦 OI History summary -> total_saved={summary['oi_history']} | "
#         f"fetches={summary['oi_history_fetches']}"
#     )
#
#     return summary