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
    EXCHANGES = params.get("exchanges", ["Binance", "Bybit"])  # Individual exchanges
    SYMBOLS = params.get("symbols", ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"])  # Base assets for aggregated data
    # Use intervals directly - API accepts "1h", "4h", "6h", "8h", "12h", "1d", "1w" format
    INTERVALS = params.get("intervals", ["1h", "4h", "6h", "8h", "12h", "1d", "1w"])
    RANGES = params.get("ranges", ["0.5", "1", "2", "5"])

    # Calculate time range for historical data
    DAYS_BACK = params.get("days_back", 7)  # Use 7 days back to avoid API limitations
    end_time = params.get("end_time", int(datetime.now().timestamp() * 1000))
    start_time = params.get("start_time")
    if not start_time:
        start_time = int((datetime.now() - timedelta(days=DAYS_BACK)).timestamp() * 1000)

    # For spot_aggregated_ask_bids_history endpoint, data is only available from May 2024
    # May 1, 2024 = 1714521600000 in milliseconds
    min_data_timestamp = 1714521600000
    current_time = int(datetime.now().timestamp() * 1000)

    if start_time < min_data_timestamp:
        logger.warning(f"⚠️ Adjusting start_time from {start_time} to {min_data_timestamp} (May 1, 2024) because historical data is only available from this date")
        start_time = min_data_timestamp

    # Validate end_time
    if end_time < min_data_timestamp:
        logger.warning(f"⚠️ End time {end_time} is before data availability date (May 1, 2024). No data will be returned.")
        return {
            "aggregated_ask_bids_history": 0,
            "aggregated_ask_bids_history_duplicates": 0,
            "fetches": 0,
            "errors": 0,
            "message": "No data available - requested time range is before May 2024"
        }

    summary = {
        "aggregated_ask_bids_history": 0,
        "aggregated_ask_bids_history_duplicates": 0,
        "fetches": 0,
        "errors": 0
    }

    logger.info(f"Starting Spot Aggregated Ask Bids History pipeline for exchanges: {EXCHANGES}")

    # Try with aggregated exchanges first
    exchange_list = ",".join(EXCHANGES)  # "Binance,Bybit"

    for symbol in SYMBOLS:
        for interval in INTERVALS:
            for range_percent in RANGES:
                try:
                    logger.info(f"Fetching aggregated ask bids history for {exchange_list} {symbol} {interval} range={range_percent}")
                    logger.debug(f"  Parameters: start_time={start_time}, end_time={end_time}")

                    data = client.get_spot_aggregated_ask_bids_history(
                        exchange_list=exchange_list,  # Use comma-separated exchanges
                            symbol=symbol,
                            interval=interval,
                            start_time=start_time,
                            end_time=end_time,
                            range_percent=range_percent
                        )

                        logger.debug(f"  API returned {len(data) if data else 0} records")

                        if data:
                            # Process and insert data with duplicate checking for each exchange
                            total_saved = 0
                            total_duplicates = 0

                            for exchange in EXCHANGES:
                                result = repo.upsert_spot_aggregated_ask_bids_history_batch(
                                    exchange, symbol, interval, range_percent, data
                                )
                                logger.info(
                                    f"✅ aggregated_ask_bids_history[{exchange}:{symbol}:{interval}:range={range_percent}]: "
                                    f"received={len(data)}, saved={result['spot_aggregated_ask_bids_history']}, duplicates={result['spot_aggregated_ask_bids_history_duplicates']}"
                                )
                                total_saved += result['spot_aggregated_ask_bids_history']
                                total_duplicates += result['spot_aggregated_ask_bids_history_duplicates']

                            summary["aggregated_ask_bids_history"] += total_saved
                            summary["aggregated_ask_bids_history_duplicates"] += total_duplicates
                        else:
                            # Check if this is early 2024 when data wasn't available
                            batch_start_date = datetime.fromtimestamp(start_time / 1000)
                            if batch_start_date.year == 2024 and batch_start_date.month < 5:
                                logger.info(
                                    f"ℹ️ aggregated_ask_bids_history[{exchange_list}:{symbol}:{interval}:range={range_percent}]: "
                                    f"No data available for {batch_start_date.strftime('%B %Y')} (Data available from May 2024)"
                                )
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