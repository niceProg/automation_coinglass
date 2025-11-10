"""Bitcoin Market Price Pipeline"""

import logging
from typing import Any, Dict, List
from datetime import datetime, timedelta
from app.repositories.cryptoquant_repository import CryptoQuantRepository
from app.core.config import Settings

logger = logging.getLogger(__name__)
settings = Settings()


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Bitcoin Market Price Pipeline
    Cadence: Every 1 day (price data)
    Endpoint: /api/cryptoquant/btc-market-price

    Retrieves Bitcoin market price data (OHLC) for price overlay
    on CDD charts and other market analysis.
    """
    repo = CryptoQuantRepository(conn, logger)

    # Pipeline parameters
    DAYS_BACK = params.get("days_back", 7)  # 7 days historical data to avoid API limits

    summary = {
        "btc_market_price": 0,
        "btc_market_price_duplicates": 0,
        "fetches": 1
    }

    logger.info(f"Starting Bitcoin Market Price pipeline")
    logger.info(f"Fetching {DAYS_BACK} days of Bitcoin price data")

    try:
        logger.info(f"Fetching Bitcoin Market Price data")

        rows = client.get_btc_market_price()

        if rows:
            saved = repo.upsert_btc_market_price_batch(rows)
            logger.info(
                f"✅ btc_market_price: "
                f"received={len(rows)}, saved={saved.get('btc_market_price', 0)}, duplicates={saved.get('btc_market_price_duplicates', 0)}"
            )
            # Handle both old int format and new dict format for backward compatibility
            if isinstance(saved, dict):
                summary["btc_market_price"] += saved.get("btc_market_price", 0)
                if saved.get("btc_market_price_duplicates", 0) > 0:
                    summary["btc_market_price_duplicates"] = saved.get("btc_market_price_duplicates", 0)
            else:
                summary["btc_market_price"] += saved
        else:
            logger.warning(f"No data returned for Bitcoin Market Price")

    except Exception as e:
        logger.warning(f"Error fetching Bitcoin Market Price: {e}")

    if summary.get("btc_market_price_duplicates", 0) > 0:
        logger.info(
            f"📦 Bitcoin Market Price pipeline completed. Total records saved: {summary['btc_market_price']}, "
            f"duplicates={summary['btc_market_price_duplicates']} (fetches:{summary['fetches']}) ✅"
        )
    else:
        logger.info(
            f"📦 Bitcoin Market Price pipeline completed. Total records saved: {summary['btc_market_price']} (fetches:{summary['fetches']}) ✅"
        )

    return summary