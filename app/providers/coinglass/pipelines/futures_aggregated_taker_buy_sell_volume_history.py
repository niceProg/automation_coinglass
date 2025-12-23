"""Futures Aggregated Taker Buy/Sell Volume History Pipeline"""

import logging
from typing import Any, Dict, List
from datetime import datetime, timedelta
from app.repositories.coinglass_repository import CoinglassRepository
from app.core.config import Settings

logger = logging.getLogger(__name__)
settings = Settings()


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Futures Aggregated Taker Buy/Sell Volume History Pipeline
    Cadence: Every 15 minutes
    Endpoint: /api/futures/aggregated-taker-buy-sell-volume/history

    Retrieves historical data for the long/short ratio of aggregated taker buy/sell
    volumes across multiple futures exchanges, providing market-wide trading pressure
    analysis.

    Based on the API documentation from futures.md:
    - Returns aggregated buy/sell volumes with timestamps
    - Support for multiple exchanges (Binance, OKX, Bybit, etc.) - processed individually
    - Various intervals from 1m to 1w
    - Support for USD or coin units
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters
    EXCHANGES = params.get("exchanges", ["Binance", "OKX", "Bybit"])  # Individual exchanges
    SYMBOLS = params.get("symbols", ["BTC", "ETH"])  # Trading coins - Sementara BTC & ETH dulu
    INTERVALS = params.get("intervals", ["1h", "4h", "6h", "8h", "12h", "1d", "1w"])  # API intervals
    UNITS = params.get("units", ["usd", "coin"])  # Data units
    LIMIT = params.get("limit", 1000)  # Max results per request

    # Calculate time range for historical data
    DAYS_BACK = params.get("days_back", 30)  # 30 days back for good coverage
    end_time = params.get("end_time", int(datetime.now().timestamp() * 1000))
    start_time = params.get("start_time")
    if not start_time:
        start_time = int((datetime.now() - timedelta(days=DAYS_BACK)).timestamp() * 1000)

    summary = {
        "futures_aggregated_taker_buy_sell_volume_history": 0,
        "futures_aggregated_taker_buy_sell_volume_history_duplicates": 0,
        "fetches": 0,
        "errors": 0
    }

    logger.info(f"Starting Futures Aggregated Taker Buy/Sell Volume History pipeline")

    for exchange in EXCHANGES:
        logger.info(f"🔄 Processing exchange: {exchange}")

        for symbol in SYMBOLS:
            for interval in INTERVALS:
                for unit in UNITS:
                    try:
                        logger.info(f"Fetching futures aggregated taker volume for {exchange} {symbol} {interval} unit={unit}")

                        data = client.get_futures_aggregated_taker_buy_sell_volume_history(
                            exchange_list=exchange,
                            symbol=symbol,
                            interval=interval,
                            start_time=start_time,
                            end_time=end_time,
                            limit=LIMIT,
                            unit=unit
                        )

                        if data:
                            # Process and insert data with duplicate checking
                            saved = repo.upsert_futures_aggregated_taker_buy_sell_volume_history_batch(
                                exchange, symbol, interval, unit, data
                            )
                            logger.info(
                                f"✅ futures_aggregated_taker_buy_sell_volume_history[{exchange}:{symbol}:{interval}:unit={unit}]: "
                                f"received={len(data)}, saved={saved.get('futures_aggregated_taker_buy_sell_volume_history', 0)}, duplicates={saved.get('futures_aggregated_taker_buy_sell_volume_history_duplicates', 0)}"
                            )
                            # Handle both old int format and new dict format for backward compatibility
                            if isinstance(saved, dict):
                                summary["futures_aggregated_taker_buy_sell_volume_history"] += saved.get("futures_aggregated_taker_buy_sell_volume_history", 0)
                                if saved.get("futures_aggregated_taker_buy_sell_volume_history_duplicates", 0) > 0:
                                    summary["futures_aggregated_taker_buy_sell_volume_history_duplicates"] = summary.get("futures_aggregated_taker_buy_sell_volume_history_duplicates", 0) + saved.get("futures_aggregated_taker_buy_sell_volume_history_duplicates", 0)
                            else:
                                summary["futures_aggregated_taker_buy_sell_volume_history"] += saved
                        else:
                            logger.warning(
                                f"No data returned for futures aggregated taker volume: {exchange} {symbol} {interval} unit={unit}"
                            )

                        summary["fetches"] += 1

                    except Exception as e:
                        logger.warning(
                            f"Error fetching futures aggregated taker volume for {exchange} {symbol}: {e}"
                        )
                        summary["fetches"] += 1
                        continue

        logger.info(f"✅ Completed processing for exchange: {exchange}")

    logger.info(f"📦 Futures Aggregated Taker Buy/Sell Volume History pipeline completed. Total records saved: {summary['futures_aggregated_taker_buy_sell_volume_history']}, duplicates={summary['futures_aggregated_taker_buy_sell_volume_history_duplicates']} ✅")
    return summary