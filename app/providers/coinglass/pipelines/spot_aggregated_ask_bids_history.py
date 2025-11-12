"""Spot Aggregated Ask Bids History Pipeline"""

import logging
from typing import Any, Dict, List
from datetime import datetime, timedelta
from app.repositories.coinglass_repository import CoinglassRepository
from app.core.config import Settings

logger = logging.getLogger(__name__)
settings = Settings()


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Spot Aggregated Ask Bids History Pipeline
    Cadence: Every 15 minutes
    Endpoint: /api/spot/orderbook/aggregated-ask-bids-history

    Retrieves aggregated historical orderbook bid/ask data across multiple exchanges,
    providing market-wide depth and liquidity analysis for spot trading.
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters
    EXCHANGE_LISTS = params.get("exchange_lists", [
        "ALL",  # All supported exchanges
        "Binance,OKX,Bybit",  # Top 3 exchanges
        "Binance,OKX,Bybit,Bitfinex,Kucoin",  # Top 5 exchanges
    ])
    SYMBOLS = params.get("symbols", ["BTC", "ETH", "SOL"])  # Base assets for aggregated data
    INTERVALS = params.get("intervals", ["h1", "h4", "h24", "d1"])  # Use API format
    RANGES = params.get("ranges", ["0.5", "1", "2", "5"])

    # Calculate time range for historical data
    DAYS_BACK = params.get("days_back", 30)  # 30 days back for good coverage
    end_time = params.get("end_time", int(datetime.now().timestamp() * 1000))
    start_time = params.get("start_time")
    if not start_time:
        start_time = int((datetime.now() - timedelta(days=DAYS_BACK)).timestamp() * 1000)

    summary = {
        "aggregated_ask_bids_history": 0,
        "aggregated_ask_bids_history_duplicates": 0,
        "fetches": 0,
        "errors": 0
    }

    logger.info(f"Starting Spot Aggregated Ask Bids History pipeline for exchange lists: {EXCHANGE_LISTS}")

    for exchange_list in EXCHANGE_LISTS:
        for symbol in SYMBOLS:
            for interval in INTERVALS:
                for range_percent in RANGES:
                    try:
                        logger.info(f"Fetching aggregated ask bids history for {exchange_list} {symbol} {interval} range={range_percent}")

                        data = client.get_spot_aggregated_ask_bids_history(
                            exchange_list=exchange_list,
                            symbol=symbol,
                            interval=interval,
                            start_time=start_time,
                            end_time=end_time,
                            range_percent=range_percent
                        )

                        if data:
                            # Process and insert data with duplicate checking
                            result = repo.upsert_spot_aggregated_ask_bids_history_batch(
                                exchange_list, symbol, interval, range_percent, data
                            )
                            logger.info(
                                f"✅ aggregated_ask_bids_history[{exchange_list}:{symbol}:{interval}:range={range_percent}]: "
                                f"received={len(data)}, saved={result['spot_aggregated_ask_bids_history']}, duplicates={result['spot_aggregated_ask_bids_history_duplicates']}"
                            )
                            summary["aggregated_ask_bids_history"] += result['spot_aggregated_ask_bids_history']
                            summary["aggregated_ask_bids_history_duplicates"] += result['spot_aggregated_ask_bids_history_duplicates']
                        else:
                            logger.info(
                                f"⚠️ aggregated_ask_bids_history[{exchange_list}:{symbol}:{interval}:range={range_percent}]: No data (skipped)"
                            )

                        summary["fetches"] += 1

                    except Exception as e:
                        logger.warning(
                            f"⚠️ aggregated_ask_bids_history[{exchange_list}:{symbol}:{interval}:range={range_percent}]: Exception: {e} (skipped)"
                        )
                        summary["errors"] += 1
                        continue

    logger.info(f"Spot Aggregated Ask Bids History pipeline completed: {summary}")
    return summary