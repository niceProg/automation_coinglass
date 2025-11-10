"""Futures Volume Footprint History Pipeline"""

import logging
from typing import Any, Dict, List
from datetime import datetime, timedelta
from app.repositories.coinglass_repository import CoinglassRepository
from app.core.config import Settings

logger = logging.getLogger(__name__)
settings = Settings()


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Futures Volume Footprint History Pipeline
    Cadence: Every 5 minutes
    Endpoint: /api/futures/volume/footprint-history

    Retrieves futures volume footprint data showing taker buy/sell volumes
    at different price ranges for specific exchanges and symbols.
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters
    EXCHANGES = params.get("exchanges", ["Binance", "Bybit"])  # Only Binance and Bybit supported
    SYMBOLS = params.get("symbols", ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "HYPEUSDT", "BNBUSDT", "DOGEUSDT"])
    INTERVALS = params.get("intervals", ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"])
    LIMIT = params.get("limit", 1000)  # Default and maximum limit

    # Calculate start_time for historical data if not provided
    HOURS_BACK = params.get("hours_back", 24)  # 24 hours back for good data coverage
    start_time = params.get("start_time")
    if not start_time:
        start_time = int((datetime.now() - timedelta(hours=HOURS_BACK)).timestamp() * 1000)

    end_time = params.get("end_time", int(datetime.now().timestamp() * 1000))

    summary = {
        "footprint_history": 0,
        "fetches": 0,
        "errors": 0
    }

    logger.info(f"Starting Futures Volume Footprint History pipeline for exchanges: {EXCHANGES}")

    for exchange in EXCHANGES:
        for symbol in SYMBOLS:
            for interval in INTERVALS:
                try:
                    logger.info(f"Fetching footprint history for {exchange} {symbol} {interval}")

                    data = client.get_futures_footprint_history(
                        exchange=exchange,
                        symbol=symbol,
                        interval=interval,
                        start_time=start_time,
                        end_time=end_time,
                        limit=LIMIT
                    )

                    if data:
                        # Process and insert data
                        repo.insert_futures_footprint_history(exchange, symbol, interval, data)
                        summary["footprint_history"] += len(data)
                        logger.info(f"Inserted {len(data)} footprint records for {exchange} {symbol} {interval}")
                    else:
                        logger.info(f"No footprint data available for {exchange} {symbol} {interval}")

                    summary["fetches"] += 1

                except Exception as e:
                    logger.error(f"Error fetching footprint history for {exchange} {symbol} {interval}: {e}")
                    summary["errors"] += 1

    logger.info(f"Futures Volume Footprint History pipeline completed: {summary}")
    return summary