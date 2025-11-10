"""Exchange Inflow CDD Pipeline"""

import logging
from typing import Any, Dict, List
from datetime import datetime, timedelta
from app.repositories.cryptoquant_repository import CryptoQuantRepository
from app.core.config import Settings

logger = logging.getLogger(__name__)
settings = Settings()


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Exchange Inflow CDD Pipeline
    Cadence: Every 1 hour (real-time data)
    Endpoint: /api/cryptoquant/exchange-inflow-cdd

    Retrieves Exchange Inflow CDD data for various exchanges and intervals,
    measuring the age of Bitcoin coins flowing into exchanges.
    High CDD indicates old coins (long-term holders) moving to exchanges,
    suggesting potential selling pressure and distribution.
    """
    repo = CryptoQuantRepository(conn, logger)

    # Pipeline parameters
    EXCHANGES = params.get("exchanges", [
        "all_exchange",      # All supported exchanges (complete aggregation)
        "spot_exchange",     # Spot exchanges only
        "derivative_exchange",  # Derivative exchanges only
        "binance",           # Individual exchanges
        "kraken",
        "bybit",
        "gemini",
        "bitfinex",
        "kucoin",
        "bitstamp",
        "mexc"
    ])
    INTERVALS = params.get("intervals", ["day", "week"])  # Daily and weekly intervals using CryptoQuant format
    DAYS_BACK = params.get("days_back", 30)  # 30 days historical data

    # Calculate date range
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d")

    summary = {
        "exchange_inflow_cdd": 0,
        "exchange_inflow_cdd_duplicates": 0,
        "fetches": 0
    }

    logger.info(f"Starting Exchange Inflow CDD pipeline for exchanges: {EXCHANGES}")
    logger.info(f"Date range: {start_date} to {end_date}")

    for exchange in EXCHANGES:
        for interval in INTERVALS:
            try:
                logger.info(f"Fetching Exchange Inflow CDD for {exchange} {interval}")

                # Convert date format for CryptoQuant API
                start_date_api = start_date.replace("-", "")

                rows = client.get_exchange_inflow_cdd(
                    exchange=exchange,
                    start_date=start_date_api,
                    interval=interval
                )

                if rows:
                    saved = repo.upsert_exchange_inflow_cdd_batch(
                        exchange, interval, rows
                    )
                    logger.info(
                        f"✅ exchange_inflow_cdd[{exchange}:{interval}]: "
                        f"received={len(rows)}, saved={saved.get('exchange_inflow_cdd', 0)}, duplicates={saved.get('exchange_inflow_cdd_duplicates', 0)}"
                    )
                    # Handle both old int format and new dict format for backward compatibility
                    if isinstance(saved, dict):
                        summary["exchange_inflow_cdd"] += saved.get("exchange_inflow_cdd", 0)
                        if saved.get("exchange_inflow_cdd_duplicates", 0) > 0:
                            summary["exchange_inflow_cdd_duplicates"] = summary.get("exchange_inflow_cdd_duplicates", 0) + saved.get("exchange_inflow_cdd_duplicates", 0)
                    else:
                        summary["exchange_inflow_cdd"] += saved
                else:
                    logger.warning(f"No data returned for Exchange Inflow CDD: {exchange} {interval}")

                summary["fetches"] += 1

            except Exception as e:
                logger.warning(f"Error fetching Exchange Inflow CDD for {exchange} {interval}: {e}")
                summary["fetches"] += 1
                continue

    if summary.get("exchange_inflow_cdd_duplicates", 0) > 0:
        logger.info(
            f"📦 Exchange Inflow CDD pipeline completed. Total records saved: {summary['exchange_inflow_cdd']}, "
            f"duplicates={summary['exchange_inflow_cdd_duplicates']} (fetches:{summary['fetches']}) ✅"
        )
    else:
        logger.info(
            f"📦 Exchange Inflow CDD pipeline completed. Total records saved: {summary['exchange_inflow_cdd']} (fetches:{summary['fetches']}) ✅"
        )

    return summary