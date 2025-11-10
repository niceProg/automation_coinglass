# # app/providers/coinglass/pipelines/supported_exchange_pairs.py
# # import logging
# # from typing import Any, Dict, List
# # from app.repositories.coinglass_repository import CoinglassRepository

# # logger = logging.getLogger(__name__)


# # def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
# #     """
# #     Supported Exchange Pairs Pipeline
# #     Cadence: Daily (exchange pairs don't change frequently)
# #     Fetches supported exchanges and their trading pairs for futures market
# #     """
# #     repo = CoinglassRepository(conn, logger)

# #     summary = {
# #         "supported_exchange_pairs": 0,
# #         "exchanges_fetched": 0,
# #         "total_pairs": 0
# #     }

# #     try:
# #         # Fetch supported exchange pairs from API
# #         logger.info("Fetching supported exchange pairs from Coinglass API...")
# #         data = client.get_supported_exchange_pairs()

# #         if data and isinstance(data, dict):
# #             # Calculate total pairs across all exchanges
# #             total_pairs = sum(len(pairs) for pairs in data.values())
# #             summary["total_pairs"] = total_pairs
# #             summary["exchanges_fetched"] = len(data)

# #             # Save to database
# #             saved = repo.upsert_supported_exchange_pairs(data)
# #             summary["supported_exchange_pairs"] = saved

# #             logger.info(
# #                 f"✅ Supported Exchange Pairs -> "
# #                 f"exchanges={len(data)}, total_pairs={total_pairs}, saved={saved}"
# #             )

# #             # Log exchange breakdown
# #             for exchange_name, pairs in data.items():
# #                 logger.info(f"   📊 {exchange_name}: {len(pairs)} pairs")

# #         else:
# #             logger.warning("⚠️ No supported exchange pairs data received from API")

# #     except Exception as e:
# #         logger.error(
# #             f"❌ Error in supported_exchange_pairs pipeline - "
# #             f"Type: {type(e).__name__}, Message: {str(e)}"
# #         )
# #         summary["error"] = str(e)

# #     logger.info(
# #         f"📦 Supported Exchange Pairs summary -> "
# #         f"saved={summary['supported_exchange_pairs']}, "
# #         f"exchanges={summary['exchanges_fetched']}, "
# #         f"total_pairs={summary['total_pairs']}"
# #     )

# #     return summary