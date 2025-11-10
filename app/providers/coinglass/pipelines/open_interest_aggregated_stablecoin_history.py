"""Open Interest Aggregated Stablecoin History Pipeline"""

import logging
from typing import Any, Dict, List
from datetime import datetime, timedelta
from app.repositories.coinglass_repository import CoinglassRepository
from app.core.config import Settings

logger = logging.getLogger(__name__)
settings = Settings()


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Open Interest Aggregated Stablecoin History Pipeline
    Cadence: Every 5 minutes (OHLC data)
    Endpoint: /api/futures/open-interest/aggregated-stablecoin-history

    Retrieves OHLC data for aggregated stablecoin margin open interest across multiple exchanges,
    providing comprehensive view of stablecoin-backed positions.
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters
    EXCHANGES = params.get("exchanges", ["Binance", "Bybit"])  # Individual exchanges
    SYMBOLS = params.get("symbols", ["BTC", "ETH"])  # Trading symbols
    INTERVALS = params.get("intervals", ["1h", "4h", "1d"])  # Time intervals for OHLC data

    # Calculate start_time for historical data if not provided
    HOURS_BACK = params.get("hours_back", 24)  # 24 hours back for OHLC data
    start_time = params.get("start_time")
    if not start_time:
        start_time = int((datetime.now() - timedelta(hours=HOURS_BACK)).timestamp() * 1000)

    summary = {
        "open_interest_aggregated_stablecoin_history": 0,
        "open_interest_aggregated_stablecoin_history_duplicates": 0,
        "fetches": 0
    }

    logger.info(f"Starting Open Interest Aggregated Stablecoin History pipeline for exchanges: {EXCHANGES}")

    for symbol in SYMBOLS:
        for interval in INTERVALS:
            for exchange in EXCHANGES:
                try:
                    logger.info(f"Fetching aggregated stablecoin OI OHLC for {exchange} {symbol} {interval}")
                    rows = client.get_open_interest_aggregated_stablecoin_history(
                        exchange_list=exchange,
                        symbol=symbol,
                        interval=interval,
                        start_time=start_time
                    )

                    if rows:
                        saved = repo.upsert_open_interest_aggregated_stablecoin_history(
                            exchange, symbol, interval, rows
                        )
                        logger.info(
                            f"✅ open_interest_aggregated_stablecoin_history[{exchange}:{symbol}:{interval}]: "
                            f"received={len(rows)}, saved={saved.get('open_interest_aggregated_stablecoin_history', 0)}, duplicates={saved.get('open_interest_aggregated_stablecoin_history_duplicates', 0)}"
                        )
                        # Handle both old int format and new dict format for backward compatibility
                        if isinstance(saved, dict):
                            summary["open_interest_aggregated_stablecoin_history"] += saved.get("open_interest_aggregated_stablecoin_history", 0)
                            if saved.get("open_interest_aggregated_stablecoin_history_duplicates", 0) > 0:
                                summary["open_interest_aggregated_stablecoin_history_duplicates"] = summary.get("open_interest_aggregated_stablecoin_history_duplicates", 0) + saved.get("open_interest_aggregated_stablecoin_history_duplicates", 0)
                        else:
                            summary["open_interest_aggregated_stablecoin_history"] += saved
                    else:
                        logger.warning(f"No data returned for aggregated stablecoin OI: {exchange} {symbol} {interval}")

                    summary["fetches"] += 1

                except Exception as e:
                    logger.warning(f"Error fetching aggregated stablecoin OI for {exchange} {symbol}: {e}")
                    summary["fetches"] += 1
                    continue

    logger.info(f"📦 Open Interest Aggregated Stablecoin History pipeline completed. Total records saved: {summary['open_interest_aggregated_stablecoin_history']}, duplicates={summary['open_interest_aggregated_stablecoin_history_duplicates']} ✅")
    return summary