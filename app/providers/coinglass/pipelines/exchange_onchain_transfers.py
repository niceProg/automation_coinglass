# import logging
# from typing import Any, Dict, List
# from datetime import datetime, timedelta
# from app.repositories.coinglass_repository import CoinglassRepository
# from app.core.config import Settings

# logger = logging.getLogger(__name__)
# settings = Settings()


# def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Exchange On-chain Transfers (ERC-20) Pipeline
#     Cadence: Real-time
#     Endpoints:
#     - /api/exchange/chain/tx/list

#     Retrieves ERC-20 on-chain transfer records for exchanges including
#     transaction details, amounts, and transfer directions.

#     Transfer types:
#     - 1 = Inflow
#     - 2 = Outflow
#     - 3 = Internal transfer
#     """
#     repo = CoinglassRepository(conn, logger)

#     # Pipeline parameters - Specific tokens only as requested
#     SYMBOLS = params.get("symbols", [
#         "USDT", "USDC", "SHIB", "MANA", "LINK", "AAVE"  # Only requested tokens
#     ])  # Specific ERC-20 tokens
#     # Minimum USD threshold for on-chain transfers
#     MIN_USD = params.get("min_usd", 100)  # Default to 100 USD minimum
#     PER_PAGE = params.get("per_page", 100)  # Max allowed by API
#     PAGE = params.get("page", 1)

#     # Calculate start_time for last 24 hours if not provided
#     HOURS_BACK = params.get("hours_back", 24)
#     start_time = params.get("start_time")
#     if not start_time:
#         start_time = int((datetime.now() - timedelta(hours=HOURS_BACK)).timestamp() * 1000)

#     summary = {
#         "exchange_onchain_transfers": 0,
#         "fetches": 0
#     }

#     logger.info(f"Starting Exchange On-chain Transfers pipeline for symbols: {SYMBOLS}")

#     for symbol in SYMBOLS:
#         try:
#             logger.info(f"Fetching on-chain transfers for {symbol} (min_usd: {MIN_USD})")
#             rows = client.get_exchange_onchain_transfers_erc20(
#                 symbol=symbol,
#                 start_time=start_time,
#                 min_usd=MIN_USD,
#                 per_page=PER_PAGE,
#                 page=PAGE
#             )

#             if rows:
#                 saved = repo.upsert_exchange_onchain_transfers(rows)
#                 summary["exchange_onchain_transfers"] += saved
#                 logger.info(f"✅ Saved {saved} on-chain transfer records for {symbol} ✅")
#             else:
#                 logger.warning(f"No data returned for on-chain transfers: {symbol}")

#             summary["fetches"] += 1

#         except Exception as e:
#             logger.warning(f"Error fetching on-chain transfers for {symbol}: {e}")
#             summary["fetches"] += 1
#             continue

#     logger.info(f"📦 Exchange On-chain Transfers pipeline completed. Total records saved: {summary['exchange_onchain_transfers']} ✅")
#     return summary