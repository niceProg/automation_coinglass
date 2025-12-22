"""Futures Aggregated Taker Volume History Pipeline"""

import logging
from typing import Any, Dict, List
from datetime import datetime, timedelta
from app.repositories.coinglass_repository import CoinglassRepository
from app.core.config import Settings

logger = logging.getLogger(__name__)
settings = Settings()


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Futures Aggregated Taker Volume History Pipeline
    Cadence: Every 10 minutes
    Endpoint: /api/futures/aggregated-taker-buy-sell-volume/history

    Retrieves historical data for aggregated taker buy/sell volumes across multiple exchanges,
    providing market-wide trading volume analysis for futures markets.
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters
    EXCHANGE_LISTS = params.get("exchange_lists", [
        "Binance",
        "Binance,Bybit",
        "Binance,Bybit,OKX"
    ])  # Individual and combined exchange lists
    SYMBOLS = params.get("symbols", ["BTC", "ETH", "SOL"])  # Base assets
    INTERVALS = params.get("intervals", ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"])
    UNIT = params.get("unit", "usd")  # or "coin"
    LIMIT = params.get("limit", 1000)

    # Calculate time range for historical data
    DAYS_BACK = params.get("days_back", 30)  # 30 days back for good coverage
    end_time = params.get("end_time", int(datetime.now().timestamp() * 1000))
    start_time = params.get("start_time")
    if not start_time:
        start_time = int((datetime.now() - timedelta(days=DAYS_BACK)).timestamp() * 1000)

    summary = {
        "futures_aggregated_taker_volume_history": 0,
        "futures_aggregated_taker_volume_history_duplicates": 0,
        "fetches": 0,
        "errors": 0
    }

    logger.info(f"Starting Futures Aggregated Taker Volume History pipeline for exchange lists: {EXCHANGE_LISTS}")

    for exchange_list in EXCHANGE_LISTS:
        for symbol in SYMBOLS:
            for interval in INTERVALS:
                try:
                    logger.info(f"Fetching futures aggregated taker volume for {exchange_list} {symbol} {interval} unit={UNIT}")

                    data = client.get_futures_aggregated_taker_volume_history(
                        exchange_list=exchange_list,
                        symbol=symbol,
                        interval=interval,
                        unit=UNIT,
                        limit=LIMIT,
                        start_time=start_time,
                        end_time=end_time
                    )

                    if data:
                        # Process and insert data with duplicate checking
                        result = repo.upsert_futures_aggregated_taker_volume_history(
                            exchange_list, symbol, interval, UNIT, data
                        )
                        logger.info(
                            f"✅ futures_aggregated_taker_volume_history[{exchange_list}:{symbol}:{interval}:unit={UNIT}]: "
                            f"received={len(data)}, saved={result.get('futures_aggregated_taker_volume_history', 0)}, duplicates={result.get('futures_aggregated_taker_volume_history_duplicates', 0)}"
                        )
                        # Handle both old int format and new dict format for backward compatibility
                        if isinstance(result, dict):
                            summary["futures_aggregated_taker_volume_history"] += result.get("futures_aggregated_taker_volume_history", 0)
                            if result.get("futures_aggregated_taker_volume_history_duplicates", 0) > 0:
                                summary["futures_aggregated_taker_volume_history_duplicates"] = summary.get("futures_aggregated_taker_volume_history_duplicates", 0) + result.get("futures_aggregated_taker_volume_history_duplicates", 0)
                        else:
                            summary["futures_aggregated_taker_volume_history"] += result
                    else:
                        logger.warning(f"No data returned for futures aggregated taker volume: {exchange_list} {symbol} {interval}")

                    summary["fetches"] += 1

                except Exception as e:
                    logger.warning(f"Error fetching futures aggregated taker volume for {exchange_list} {symbol} {interval}: {e}")
                    summary["errors"] += 1
                    summary["fetches"] += 1
                    continue

    logger.info(f"📦 Futures Aggregated Taker Volume History pipeline completed. Total records saved: {summary['futures_aggregated_taker_volume_history']}, duplicates={summary['futures_aggregated_taker_volume_history_duplicates']} ✅")
    return summary