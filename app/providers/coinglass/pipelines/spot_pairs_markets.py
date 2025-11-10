import logging
from typing import Any, Dict, List
from app.repositories.coinglass_repository import CoinglassRepository
from app.core.config import Settings

logger = logging.getLogger(__name__)
settings = Settings()


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Spot Pairs Markets Pipeline
    Cadence: Every 5 minutes (real-time market data)
    Endpoint: /api/spot/pairs-markets

    Retrieves performance data for trading pairs of specific symbols across exchanges,
    including price changes, volume analysis, and net flow indicators.
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters - mandatory requirements
    SYMBOLS = params.get("symbols", ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"])  # All mandatory symbols
    TARGET_EXCHANGES = params.get("exchanges", ["Binance", "Bybit"])  # Binance and Bybit as requested

    summary = {
        "spot_pairs_markets": 0,
        "fetches": 0
    }

    logger.info(f"Starting Spot Pairs Markets pipeline for symbols: {SYMBOLS}")

    for symbol in SYMBOLS:
        try:
            logger.info(f"Fetching spot pairs markets for {symbol}")
            rows = client.get_spot_pairs_markets(symbol)

            if rows:
                # Filter for target exchanges (Binance and Bybit)
                filtered_rows = [
                    row for row in rows
                    if row.get("exchange_name") in TARGET_EXCHANGES
                ]

                if filtered_rows:
                    saved = repo.upsert_spot_pairs_markets(filtered_rows)
                    summary["spot_pairs_markets"] += saved
                    logger.info(f"✅ Saved {saved} spot pairs markets records for {symbol} on {TARGET_EXCHANGES} ✅")
                else:
                    logger.info(f"No data found for {symbol} on target exchanges: {TARGET_EXCHANGES}")
            else:
                logger.warning(f"No data returned for spot pairs markets: {symbol}")

            summary["fetches"] += 1

        except Exception as e:
            logger.warning(f"Error fetching spot pairs markets for {symbol}: {e}")
            summary["fetches"] += 1
            continue

    logger.info(f"📦 Spot Pairs Markets pipeline completed. Total records saved: {summary['spot_pairs_markets']} ✅")
    return summary