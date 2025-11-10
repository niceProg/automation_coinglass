# app/providers/coinglass/pipelines/coins_markets.py
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
#     Coins Markets Pipeline
#     Cadence: Every 5 minutes (real-time market data)
#     Fetches aggregated coin performance data across multiple timeframes
#     """
#     repo = CoinglassRepository(conn, logger)
#
#     EXCHANGE_LIST = params.get("exchange_list", "Binance,Bybit")  # Only Binance and Bybit
#     PER_PAGE = params.get("per_page", 50)
#     PAGE = params.get("page", 1)
#
#     summary = {
#         "coins_markets": 0,
#         "pages_fetched": 0,
#         "exchange_list": EXCHANGE_LIST
#     }
#
#     try:
#         logger.info(f"Fetching coins markets data for exchanges: {EXCHANGE_LIST}")
#
#         # Fetch coins markets data with pagination
#         rows = client.get_coins_markets(
#             exchange_list=EXCHANGE_LIST,
#             per_page=PER_PAGE,
#             page=PAGE
#         )
#
#         if rows and isinstance(rows, list):
#             summary["pages_fetched"] = 1
#
#             # Save to database
#             saved = repo.upsert_coins_markets(rows)
#             summary["coins_markets"] = saved
#
#             logger.info(
#                 f"✅ coins_markets: "
#                 f"received={len(rows)}, saved={saved}, page={PAGE}"
#             )
#
#             # Log top coins by market cap
#             top_coins = sorted(
#                 [r for r in rows if r.get("market_cap_usd")],
#                 key=lambda x: x.get("market_cap_usd", 0),
#                 reverse=True
#             )[:5]
#
#             for coin in top_coins:
#                 symbol = coin.get("symbol", "Unknown")
#                 market_cap = coin.get("market_cap_usd", 0)
#                 price_change_24h = coin.get("price_change_percent_24h", 0)
#                 logger.info(
#                     f"   📊 {symbol}: MC=${market_cap:,.0f}, 24h={price_change_24h:+.2f}%"
#                 )
#
#         else:
#             logger.warning("⚠️ coins_markets: No data received")
#
#     except Exception as e:
#         logger.error(
#             f"❌ coins_markets: "
#             f"Type: {type(e).__name__}, Message: {str(e)}"
#         )
#         summary["error"] = str(e)
#
#     logger.info(
#         f"📦 Coins Markets summary -> "
#         f"saved={summary['coins_markets']}, "
#         f"pages={summary['pages_fetched']}, "
#         f"exchanges={summary['exchange_list']}"
#     )
#
#     return summary