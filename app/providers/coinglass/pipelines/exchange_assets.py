# import logging
# from typing import Any, Dict, List
# from app.repositories.coinglass_repository import CoinglassRepository
# from app.core.config import Settings

# logger = logging.getLogger(__name__)
# settings = Settings()


# def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Exchange Assets Pipeline
#     Cadence: Every 1 hour
#     Endpoints:
#     - /api/exchange/assets

#     Retrieves wallet asset information for specific exchanges including
#     wallet addresses, balances, and asset details.
#     """
#     repo = CoinglassRepository(conn, logger)

#     # Pipeline parameters
#     EXCHANGES = params.get("exchanges", ["Binance", "Bybit"])  # Only Binance and Bybit
#     PER_PAGE = params.get("per_page", 10)
#     PAGE = params.get("page", 1)

#     summary = {
#         "exchange_assets": 0,
#         "fetches": 0
#     }

#     logger.info(f"Starting Exchange Assets pipeline for exchanges: {EXCHANGES}")

#     for exchange in EXCHANGES:
#         try:
#             logger.info(f"Fetching exchange assets for {exchange}")
#             rows = client.get_exchange_assets(
#                 exchange=exchange,
#                 per_page=PER_PAGE,
#                 page=PAGE
#             )

#             if rows:
#                 saved = repo.upsert_exchange_assets(exchange, rows)
#                 summary["exchange_assets"] += saved
#                 logger.info(f"✅ Saved {saved} exchange assets for {exchange} ✅")
#             else:
#                 logger.warning(f"No data returned for exchange assets: {exchange}")

#             summary["fetches"] += 1

#         except Exception as e:
#             logger.warning(f"Error fetching exchange assets for {exchange}: {e}")
#             summary["fetches"] += 1
#             continue

#     logger.info(f"📦 Exchange Assets pipeline completed. Total assets saved: {summary['exchange_assets']} ✅")
#     return summary