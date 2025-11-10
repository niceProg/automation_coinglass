import logging
from typing import Any, Dict, List
from app.repositories.coinglass_repository import CoinglassRepository

logger = logging.getLogger(__name__)


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Spot Coins Markets Pipeline
    Cadence: Every 5 minutes (real-time market data)
    Endpoint: /api/spot/coins-markets

    Retrieves comprehensive market performance data for all available coins,
    including price changes, volume analysis, and market sentiment indicators.
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters
    PER_PAGE = params.get("per_page", 100)  # Maximum per page
    PAGE = params.get("page", 1)
    MAX_PAGES = params.get("max_pages", 10)  # Limit pages to avoid API overuse

    summary = {
        "spot_coins_markets": 0,
        "fetches": 0
    }

    logger.info(f"Starting Spot Coins Markets pipeline")

    try:
        logger.info(f"Fetching spot coins markets data (page {PAGE}, per_page={PER_PAGE})")
        rows = client.get_spot_coins_markets(per_page=PER_PAGE, page=PAGE)

        if rows:
            # Filter for major coins (BTC, ETH) and high market cap coins if needed
            target_symbols = params.get("symbols", ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"])
            filtered_rows = [row for row in rows if row.get("symbol") in target_symbols]

            # If no specific symbols requested, use all returned data
            if not target_symbols:
                filtered_rows = rows

            if filtered_rows:
                saved = repo.upsert_spot_coins_markets(filtered_rows)
                summary["spot_coins_markets"] += saved
                logger.info(f"✅ Saved {saved} spot coins markets records ✅")
            else:
                logger.info("No matching symbols found in spot coins markets data")
        else:
            logger.warning("No data returned for spot coins markets")

        summary["fetches"] += 1

    except Exception as e:
        logger.warning(f"Error fetching spot coins markets: {e}")
        summary["fetches"] += 1

    logger.info(f"📦 Spot Coins Markets pipeline completed. Total records saved: {summary['spot_coins_markets']} ✅")
    return summary