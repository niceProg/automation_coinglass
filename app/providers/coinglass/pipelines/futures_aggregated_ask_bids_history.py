"""Futures Aggregated Ask Bids History Pipeline"""

import logging
from typing import Any, Dict, List
from datetime import datetime, timedelta
from app.repositories.coinglass_repository import CoinglassRepository
from app.core.config import Settings

logger = logging.getLogger(__name__)
settings = Settings()


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Futures Aggregated Ask Bids History Pipeline
    Cadence: Every 15 minutes
    Endpoint: /api/futures/orderbook/aggregated-ask-bids-history

    Retrieves aggregated historical orderbook bid/ask data across multiple exchanges
    for futures trading, providing market-wide depth and liquidity analysis.

    Based on the API documentation from panduan.md:
    - Returns aggregated long/short amounts within specific price ranges
    - Support for multiple exchanges (Binance, OKX, Bybit, etc.)
    - Various intervals from 1m to 1w
    - Depth percentages from 0.25% to 10%
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters
    EXCHANGES = params.get("exchanges", ["Binance", "Bybit"])  # List of exchanges to run sequentially
    SYMBOLS = params.get("symbols", ["BTC", "ETH", "SOL"])  # Trading coins
    INTERVALS = params.get("intervals", ["1h", "4h", "6h", "8h", "12h", "1d", "1w"])  # API intervals
    RANGES = params.get("ranges", ["0.25", "0.5", "1", "2", "5"])  # Depth percentages (0.25, 0.5, 0.75, 1, 2, 3, 5, 10)
    LIMIT = params.get("limit", 1000)  # Max results per request

    # Calculate time range for historical data
    DAYS_BACK = params.get("days_back", 30)  # 30 days back for good coverage
    end_time = params.get("end_time", int(datetime.now().timestamp() * 1000))
    start_time = params.get("start_time")
    if not start_time:
        start_time = int((datetime.now() - timedelta(days=DAYS_BACK)).timestamp() * 1000)

    summary = {
        "futures_aggregated_ask_bids_history": 0,
        "futures_aggregated_ask_bids_history_duplicates": 0,
        "fetches": 0,
        "errors": 0
    }

    logger.info(f"Starting Futures Aggregated Ask Bids History pipeline for exchanges: {EXCHANGES}")

    # Process each exchange sequentially
    for exchange in EXCHANGES:
        logger.info(f"🔄 Processing exchange: {exchange}")

        for symbol in SYMBOLS:
            for interval in INTERVALS:
                for range_percent in RANGES:
                    try:
                        logger.info(f"Fetching futures aggregated ask bids history for {exchange} {symbol} {interval} range={range_percent}")

                        data = client.get_futures_aggregated_ask_bids_history(
                            exchange_list=exchange,  # Single exchange
                            symbol=symbol,
                            interval=interval,
                            start_time=start_time,
                            end_time=end_time,
                            limit=LIMIT,
                            range_percent=range_percent
                        )

                        if data:
                            # Process and insert data with duplicate checking
                            result = repo.upsert_futures_aggregated_ask_bids_history_batch(
                                exchange, symbol, interval, range_percent, data
                            )
                            logger.info(
                                f"✅ futures_aggregated_ask_bids_history[{exchange}:{symbol}:{interval}:range={range_percent}]: "
                                f"received={len(data)}, saved={result['futures_aggregated_ask_bids_history']}, duplicates={result['futures_aggregated_ask_bids_history_duplicates']}"
                            )
                            summary["futures_aggregated_ask_bids_history"] += result['futures_aggregated_ask_bids_history']
                            summary["futures_aggregated_ask_bids_history_duplicates"] += result['futures_aggregated_ask_bids_history_duplicates']
                        else:
                            logger.info(
                                f"⚠️ futures_aggregated_ask_bids_history[{exchange}:{symbol}:{interval}:range={range_percent}]: No data (skipped)"
                            )

                        summary["fetches"] += 1

                    except Exception as e:
                        logger.warning(
                            f"⚠️ futures_aggregated_ask_bids_history[{exchange}:{symbol}:{interval}:range={range_percent}]: Exception: {e} (skipped)"
                        )
                        summary["errors"] += 1
                        continue

        logger.info(f"✅ Completed processing for exchange: {exchange}")

    logger.info(f"Futures Aggregated Ask Bids History pipeline completed: {summary}")
    return summary