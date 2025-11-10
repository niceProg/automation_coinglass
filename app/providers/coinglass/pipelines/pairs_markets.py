# # app/providers/coinglass/pipelines/pairs_markets.py
# # import logging
# # from typing import Any, Dict, List
# # from app.repositories.coinglass_repository import CoinglassRepository

# # logger = logging.getLogger(__name__)


# # def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
# #     """
# #     Pairs Markets Pipeline
# #     Cadence: Every 5 minutes (real-time market data)
# #     Fetches comprehensive trading pair performance data across exchanges
# #     """
# #     repo = CoinglassRepository(conn, logger)

# #     SYMBOLS = params.get("symbols", ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"])
# #     summary = {
# #         "pairs_markets": 0,
# #         "symbols_processed": 0,
# #         "total_pairs_fetched": 0
# #     }

# #     for symbol in SYMBOLS:
# #         try:
# #             logger.info(f"Fetching pairs markets data for {symbol}...")

# #             # Fetch pairs markets data for the symbol
# #             rows = client.get_pairs_markets(symbol=symbol)

# #             if rows and isinstance(rows, list):
# #                 summary["total_pairs_fetched"] += len(rows)
# #                 summary["symbols_processed"] += 1

# #                 # Save to database
# #                 saved = repo.upsert_pairs_markets(rows)
# #                 summary["pairs_markets"] += saved

# #                 logger.info(
# #                     f"✅ pairs_markets[{symbol}]: "
# #                     f"received={len(rows)}, saved={saved}"
# #                 )

# #                 # Log exchange breakdown
# #                 exchange_counts = {}
# #                 for row in rows:
# #                     exchange = row.get("exchange_name", "Unknown")
# #                     exchange_counts[exchange] = exchange_counts.get(exchange, 0) + 1

# #                 for exchange, count in exchange_counts.items():
# #                     logger.info(f"   📊 {exchange}: {count} pairs")

# #             else:
# #                 logger.warning(f"⚠️ pairs_markets[{symbol}]: No data received")

# #         except Exception as e:
# #             logger.error(
# #                 f"❌ pairs_markets[{symbol}]: "
# #                 f"Type: {type(e).__name__}, Message: {str(e)}"
# #             )
# #             continue

# #     logger.info(
# #         f"📦 Pairs Markets summary -> "
# #         f"saved={summary['pairs_markets']}, "
# #         f"symbols={summary['symbols_processed']}, "
# #         f"total_pairs={summary['total_pairs_fetched']}"
# #     )

# #     return summary