"""Spot Ask Bids History Pipeline"""

import logging
from typing import Any, Dict, List
from datetime import datetime, timedelta
from app.repositories.coinglass_repository import CoinglassRepository
from app.core.config import Settings

logger = logging.getLogger(__name__)
settings = Settings()


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Spot Ask Bids History Pipeline
    Cadence: Every 10 minutes
    Endpoint: /api/spot/orderbook/ask-bids-history

    Retrieves historical orderbook bid/ask data for spot markets,
    including total bid/ask amounts and quantities with depth ranges.
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters
    EXCHANGES = params.get("exchanges", ["Binance", "Bybit", "OKX"])
    SYMBOLS = params.get("symbols", ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    INTERVALS = params.get("intervals", ["1h", "4h", "1d"])
    RANGES = params.get("ranges", ["0.5", "1", "2"])

    # Calculate time range for historical data
    DAYS_BACK = params.get("days_back", 7)  # 7 days back for good coverage
    end_time = params.get("end_time", int(datetime.now().timestamp() * 1000))
    start_time = params.get("start_time")
    if not start_time:
        start_time = int((datetime.now() - timedelta(days=DAYS_BACK)).timestamp() * 1000)

    summary = {
        "ask_bids_history": 0,
        "ask_bids_history_duplicates": 0,
        "fetches": 0,
        "errors": 0
    }

    logger.info(f"Starting Spot Ask Bids History pipeline for exchanges: {EXCHANGES}")

    for exchange in EXCHANGES:
        for symbol in SYMBOLS:
            for interval in INTERVALS:
                for range_percent in RANGES:
                    try:
                        logger.info(f"Fetching ask bids history for {exchange} {symbol} {interval} range={range_percent}")

                        data = client.get_spot_ask_bids_history(
                            exchange=exchange,
                            symbol=symbol,
                            interval=interval,
                            start_time=start_time,
                            end_time=end_time,
                            range_percent=range_percent
                        )

                        if data:
                            # Process and insert data with duplicate checking
                            result = repo.upsert_spot_ask_bids_history_batch(
                                exchange, interval, range_percent, data
                            )
                            logger.info(
                                f"✅ ask_bids_history[{exchange}:{symbol}:{interval}:range={range_percent}]: "
                                f"received={len(data)}, saved={result['ask_bids_history']}, duplicates={result['ask_bids_history_duplicates']}"
                            )
                            summary["ask_bids_history"] += result['ask_bids_history']
                            summary["ask_bids_history_duplicates"] += result['ask_bids_history_duplicates']
                        else:
                            logger.info(
                                f"⚠️ ask_bids_history[{exchange}:{symbol}:{interval}:range={range_percent}]: No data (skipped)"
                            )

                        summary["fetches"] += 1

                    except Exception as e:
                        logger.warning(
                            f"⚠️ ask_bids_history[{exchange}:{symbol}:{interval}:range={range_percent}]: Exception: {e} (skipped)"
                        )
                        summary["errors"] += 1
                        continue

    logger.info(f"Spot Ask Bids History pipeline completed: {summary}")
    return summary