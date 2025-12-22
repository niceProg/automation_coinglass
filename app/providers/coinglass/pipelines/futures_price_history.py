"""Futures Price History (OHLC) Pipeline"""

import logging
from typing import Any, Dict, List
from datetime import datetime, timedelta
from app.repositories.coinglass_repository import CoinglassRepository
from app.core.config import Settings

logger = logging.getLogger(__name__)
settings = Settings()


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Futures Price History (OHLC) Pipeline
    Cadence: Every 5 minutes
    Endpoint: /api/futures/price/history

    Retrieves historical open, high, low, and close (OHLC) price data for futures
    cryptocurrencies, providing comprehensive market price data for technical analysis.

    Based on the API documentation from futures.md:
    - Returns OHLC price data with volume
    - Support for multiple futures exchanges (Binance, OKX, etc.)
    - Various intervals from 1m to 1w
    - Real-time data for all API plans
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters
    EXCHANGES = params.get("exchanges", ["Binance", "OKX", "Bybit"])  # Futures exchanges
    SYMBOLS = params.get("symbols", ["BTCUSDT", "ETHUSDT", "SOLUSDT"])  # Trading pairs
    INTERVALS = params.get("intervals", ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"])  # API intervals
    LIMIT = params.get("limit", 1000)  # Max results per request

    # Calculate time range for historical data
    DAYS_BACK = params.get("days_back", 7)  # 7 days back for good coverage
    end_time = params.get("end_time", int(datetime.now().timestamp() * 1000))
    start_time = params.get("start_time")
    if not start_time:
        start_time = int((datetime.now() - timedelta(days=DAYS_BACK)).timestamp() * 1000)

    summary = {
        "futures_price_history": 0,
        "futures_price_history_duplicates": 0,
        "fetches": 0,
        "errors": 0
    }

    logger.info(f"Starting Futures Price History (OHLC) pipeline for exchanges: {EXCHANGES}")

    for exchange in EXCHANGES:
        logger.info(f"🔄 Processing exchange: {exchange}")

        for symbol in SYMBOLS:
            for interval in INTERVALS:
                try:
                    logger.info(f"Fetching futures price history for {exchange} {symbol} {interval}")

                    data = client.get_futures_price_history(
                        exchange=exchange,
                        symbol=symbol,
                        interval=interval,
                        start_time=start_time,
                        end_time=end_time,
                        limit=LIMIT
                    )

                    if data:
                        # Process and insert data with duplicate checking
                        saved = repo.upsert_futures_price_history_batch(
                            exchange, symbol, interval, data
                        )
                        logger.info(
                            f"✅ futures_price_history[{exchange}:{symbol}:{interval}]: "
                            f"received={len(data)}, saved={saved.get('futures_price_history', 0)}, duplicates={saved.get('futures_price_history_duplicates', 0)}"
                        )
                        # Handle both old int format and new dict format for backward compatibility
                        if isinstance(saved, dict):
                            summary["futures_price_history"] += saved.get("futures_price_history", 0)
                            if saved.get("futures_price_history_duplicates", 0) > 0:
                                summary["futures_price_history_duplicates"] = summary.get("futures_price_history_duplicates", 0) + saved.get("futures_price_history_duplicates", 0)
                        else:
                            summary["futures_price_history"] += saved
                    else:
                        logger.warning(
                            f"No data returned for futures price history: {exchange} {symbol} {interval}"
                        )

                    summary["fetches"] += 1

                except Exception as e:
                    logger.warning(
                        f"Error fetching futures price history for {exchange} {symbol}: {e}"
                    )
                    summary["fetches"] += 1
                    continue

        logger.info(f"✅ Completed processing for exchange: {exchange}")

    logger.info(f"📦 Futures Price History pipeline completed. Total records saved: {summary['futures_price_history']}, duplicates={summary['futures_price_history_duplicates']} ✅")
    return summary