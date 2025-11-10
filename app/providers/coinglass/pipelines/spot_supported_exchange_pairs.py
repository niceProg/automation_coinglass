# import logging
# from typing import Any, Dict, List
# from app.repositories.coinglass_repository import CoinglassRepository

# logger = logging.getLogger(__name__)


# def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Spot Supported Exchange Pairs Pipeline
#     Cadence: Every 60 minutes (reference data)
#     Endpoint: /api/spot/supported-exchange-pairs

#     Retrieves supported exchanges and their trading pairs for spot markets,
#     providing reference data for available trading pairs across exchanges.
#     """
#     repo = CoinglassRepository(conn, logger)

#     summary = {
#         "spot_supported_exchange_pairs": 0,
#         "fetches": 0
#     }

#     logger.info("Starting Spot Supported Exchange Pairs pipeline")

#     try:
#         logger.info("Fetching spot supported exchange pairs")
#         data = client.get_spot_supported_exchange_pairs()

#         if data:
#             saved = repo.upsert_spot_supported_exchange_pairs(data)
#             summary["spot_supported_exchange_pairs"] += saved
#             logger.info(f"✅ Saved {saved} spot supported exchange pairs records ✅")
#         else:
#             logger.warning("No data returned for spot supported exchange pairs")

#         summary["fetches"] += 1

#     except Exception as e:
#         logger.warning(f"Error fetching spot supported exchange pairs: {e}")
#         summary["fetches"] += 1

#     logger.info(f"📦 Spot Supported Exchange Pairs pipeline completed. Total records saved: {summary['spot_supported_exchange_pairs']} ✅")
#     return summary