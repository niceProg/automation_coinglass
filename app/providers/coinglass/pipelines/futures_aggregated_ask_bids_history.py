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

    Retrieves aggregated historical orderbook bid/ask data across multiple exchanges,
    providing market-wide depth and liquidity analysis for futures trading.
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters
    EXCHANGES = params.get("exchanges", ["Binance", "Bybit", "OKX"])  # Individual exchanges
    SYMBOLS = params.get("symbols", ["BTC", "ETH", "SOL"])  # Base assets for aggregated data
    INTERVALS = params.get("intervals", ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "12h", "1d"])  # Use API format
    RANGES = params.get("ranges", ["0.5", "1", "2", "5"])

    # Calculate time range for historical data
    DAYS_BACK = params.get("days_back", 30)  # 30 days back for good coverage
    end_time = params.get("end_time", int(datetime.now().timestamp() * 1000))
    start_time = params.get("start_time")
    if not start_time:
        start_time = int((datetime.now() - timedelta(days=DAYS_BACK)).timestamp() * 1000)

    summary = {
        "futures_aggregated_ask_bids_history": 0,
        "futures_aggregated_ask_bids_history_duplicates": 0,
        "fetches": 0
    }

    logger.info(f"Starting Futures Aggregated Ask Bids History pipeline for exchanges: {EXCHANGES}")

    for exchange in EXCHANGES:
        for symbol in SYMBOLS:
            for interval in INTERVALS:
                for range_percent in RANGES:
                    try:
                        logger.info(f"Fetching aggregated ask bids history for {exchange} {symbol} {interval} range={range_percent}")

                        data = client.get_futures_aggregated_ask_bids_history(
                            exchange_list=exchange,  # Use single exchange name
                            symbol=symbol,
                            interval=interval,
                            start_time=start_time,
                            end_time=end_time,
                            range_percent=range_percent
                        )

                        if data:
                            # Process and insert data with duplicate checking
                            result = repo.upsert_futures_aggregated_ask_bids_history(
                                exchange, symbol, interval, range_percent, data
                            )
                            logger.info(
                                f"✅ futures_aggregated_ask_bids_history[{exchange}:{symbol}:{interval}:range={range_percent}]: "
                                f"received={len(data)}, saved={result.get('futures_aggregated_ask_bids_history', 0)}, duplicates={result.get('futures_aggregated_ask_bids_history_duplicates', 0)}"
                            )
                            # Handle both old int format and new dict format for backward compatibility
                            if isinstance(result, dict):
                                summary["futures_aggregated_ask_bids_history"] += result.get("futures_aggregated_ask_bids_history", 0)
                                if result.get("futures_aggregated_ask_bids_history_duplicates", 0) > 0:
                                    summary["futures_aggregated_ask_bids_history_duplicates"] = summary.get("futures_aggregated_ask_bids_history_duplicates", 0) + result.get("futures_aggregated_ask_bids_history_duplicates", 0)
                            else:
                                summary["futures_aggregated_ask_bids_history"] += result
                        else:
                            logger.warning(f"No data returned for futures aggregated ask bids history: {exchange} {symbol} {interval} range={range_percent}")

                        summary["fetches"] += 1

                    except Exception as e:
                        logger.warning(f"Error fetching futures aggregated ask bids history for {exchange} {symbol} {interval} range={range_percent}: {e}")
                        summary["fetches"] += 1
                        continue

    logger.info(f"📦 Futures Aggregated Ask Bids History pipeline completed. Total records saved: {summary['futures_aggregated_ask_bids_history']}, duplicates={summary['futures_aggregated_ask_bids_history_duplicates']} ✅")
    return summary