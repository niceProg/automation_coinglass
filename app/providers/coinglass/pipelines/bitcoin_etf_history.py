# DISABLED - Endpoint not documented in API markdown
# import logging
# from typing import Any, Dict, List
# from app.repositories.coinglass_repository import CoinglassRepository
#
# logger = logging.getLogger(__name__)
#
#
# def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Bitcoin ETF History Pipeline
#     Cadence: Every 1 hour (historical data)
#     Endpoint: /api/etf/bitcoin/history
#
#     Retrieves historical premium/discount fluctuations for specific Bitcoin ETFs
#     including asset values, market prices, and premium/discount percentages.
#     """
#     repo = CoinglassRepository(conn, logger)
#
#     # Pipeline parameters - major Bitcoin ETFs
#     TICKERS = params.get("tickers", ["GBTC", "IBIT", "FBTC", "ARKB", "BITO", "BRRR"])
#
#     summary = {
#         "bitcoin_etf_history": 0,
#         "bitcoin_etf_history_duplicates": 0,
#         "fetches": 0
#     }
#
#     logger.info(f"Starting Bitcoin ETF History pipeline for tickers: {TICKERS}")
#
#     for ticker in TICKERS:
#         try:
#             logger.info(f"Fetching ETF history for {ticker}")
#             rows = client.get_etf_history(ticker)
#
#             if rows:
#                 result = repo.upsert_bitcoin_etf_history(ticker, rows)
#                 saved = result.get("bitcoin_etf_history", 0)
#                 duplicates = result.get("bitcoin_etf_history_duplicates", 0)
#
#                 summary["bitcoin_etf_history"] += saved
#                 summary["bitcoin_etf_history_duplicates"] += duplicates
#
#                 logger.info(
#                     f"✅ ETF history [{ticker}]: received={len(rows)}, "
#                     f"saved={saved}, duplicates={duplicates} ✅"
#                 )
#             else:
#                 logger.warning(f"No data returned for ETF history: {ticker}")
#
#             summary["fetches"] += 1
#
#         except Exception as e:
#             logger.warning(f"Error fetching ETF history for {ticker}: {e}")
#             summary["fetches"] += 1
#             continue
#
#     logger.info(
#         f"📦 Bitcoin ETF History pipeline completed. Total records saved: {summary['bitcoin_etf_history']} "
#         f"(duplicates: {summary['bitcoin_etf_history_duplicates']}) ✅"
#     )
#     return summary