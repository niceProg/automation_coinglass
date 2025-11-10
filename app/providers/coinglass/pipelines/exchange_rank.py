"""Exchange Rank Pipeline"""

import logging
from typing import Any, Dict, List
from app.repositories.coinglass_repository import CoinglassRepository
from app.core.config import Settings

logger = logging.getLogger(__name__)
settings = Settings()


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Exchange Rank Pipeline
    Cadence: Every 10 minutes (ranking data)
    Endpoint: /api/futures/exchange-rank

    Retrieves exchange rankings by open interest, volume, and liquidations,
    providing competitive insights across different trading metrics.
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters
    SYMBOLS = params.get("symbols", ["BTC", "ETH", "SOL"])  # Trading symbols

    summary = {
        "exchange_rank": 0,
        "fetches": 0
    }

    logger.info(f"Starting Exchange Rank pipeline for symbols: {SYMBOLS}")

    for symbol in SYMBOLS:
        try:
            logger.info(f"Fetching exchange rankings for {symbol}")
            rows = client.get_exchange_rank(
                symbol=symbol,
            )

            if rows:
                saved = repo.upsert_exchange_rank(rows)
                summary["exchange_rank"] += saved
                logger.info(f"✅ Saved {saved} exchange rank records for {symbol} ✅")
            else:
                logger.warning(f"No data returned for exchange rank: {symbol}")

            summary["fetches"] += 1

        except Exception as e:
            logger.warning(f"Error fetching exchange rank for {symbol}: {e}")
            summary["fetches"] += 1
            continue

    logger.info(f"📦 Exchange Rank pipeline completed. Total records saved: {summary['exchange_rank']} ✅")
    return summary