"""Spot Aggregated Taker Buy/Sell Volume History Pipeline"""

import logging
from typing import Any, Dict, List
from datetime import datetime, timedelta
from app.repositories.coinglass_repository import CoinglassRepository
from app.core.config import Settings

logger = logging.getLogger(__name__)
settings = Settings()


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Spot Aggregated Taker Buy/Sell Volume History Pipeline
    Cadence: Every 5 minutes
    Endpoint: /api/spot/aggregated-taker-buy-sell-volume/history

    Retrieves historical data for aggregated taker buy/sell volumes across
    multiple exchanges for specific symbols and time intervals.
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters
    EXCHANGE_LISTS = params.get("exchange_lists", ["Binance", "Binance,Bybit"])  # Single and multiple exchanges
    SYMBOLS = params.get("symbols", ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"])  # Base assets
    INTERVALS = params.get("intervals", ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"])
    LIMIT = params.get("limit", 1000)  # Default limit
    UNIT = params.get("unit", "usd")  # Default to USD

    # Calculate start_time for historical data if not provided
    HOURS_BACK = params.get("hours_back", 24)  # 24 hours back for good data coverage
    start_time = params.get("start_time")
    if not start_time:
        start_time = int((datetime.now() - timedelta(hours=HOURS_BACK)).timestamp() * 1000)

    end_time = params.get("end_time", int(datetime.now().timestamp() * 1000))

    summary = {
        "aggregated_taker_volume_history": 0,
        "fetches": 0,
        "errors": 0
    }

    logger.info(f"Starting Spot Aggregated Taker Volume History pipeline for symbols: {SYMBOLS}")

    for exchange_list in EXCHANGE_LISTS:
        for symbol in SYMBOLS:
            for interval in INTERVALS:
                try:
                    logger.info(f"Fetching aggregated taker volume history for exchanges={exchange_list} {symbol} {interval}")

                    data = client.get_spot_aggregated_taker_volume_history(
                        exchange_list=exchange_list,
                        symbol=symbol,
                        interval=interval,
                        start_time=start_time,
                        end_time=end_time,
                        limit=LIMIT,
                        unit=UNIT
                    )

                    if data:
                        # Process and insert data
                        repo.insert_spot_aggregated_taker_volume_history(exchange_list, symbol, interval, UNIT, data)
                        summary["aggregated_taker_volume_history"] += len(data)
                        logger.info(f"Inserted {len(data)} aggregated taker volume records for {exchange_list} {symbol} {interval}")
                    else:
                        logger.info(f"No aggregated taker volume data available for {exchange_list} {symbol} {interval}")

                    summary["fetches"] += 1

                except Exception as e:
                    logger.error(f"Error fetching aggregated taker volume history for {exchange_list} {symbol} {interval}: {e}")
                    summary["errors"] += 1

    logger.info(f"Spot Aggregated Taker Volume History pipeline completed: {summary}")
    return summary