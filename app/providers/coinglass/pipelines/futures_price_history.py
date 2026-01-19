"""Futures Price History Pipeline"""

import logging
from typing import Any, Dict, List
from datetime import datetime, timedelta
from app.repositories.coinglass_repository import CoinglassRepository
from app.core.config import settings

logger = logging.getLogger(__name__)


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Futures Price History Pipeline
    Cadence: Every 5 minutes
    Endpoint: /api/futures/price/history

    Retrieves historical OHLC price data for futures markets.
    Provides open, high, low, close prices and volume data over specified timeframes.
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters
    EXCHANGES = params.get("exchanges", settings.COINGLASS_EXCHANGES)
    raw_symbols = params.get("symbols", settings.COINGLASS_SYMBOLS)
    SYMBOLS = _ensure_usdt_pairs(raw_symbols)
    INTERVALS = params.get("intervals", ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"])
    LIMIT = params.get("limit", 1000)

    # Calculate time range for historical data
    DAYS_BACK = params.get("days_back")
    HOURS_BACK = params.get("hours_back", 24)
    end_time = params.get("end_time", int(datetime.now().timestamp() * 1000))
    start_time = params.get("start_time")
    if not start_time:
        if DAYS_BACK:
            start_time = int((datetime.now() - timedelta(days=DAYS_BACK)).timestamp() * 1000)
        else:
            start_time = int((datetime.now() - timedelta(hours=HOURS_BACK)).timestamp() * 1000)

    summary = {
        "futures_price_history": 0,
        "futures_price_history_duplicates": 0,
        "fetches": 0,
        "errors": 0
    }

    logger.info(f"Starting Futures Price History pipeline for exchanges: {EXCHANGES}")

    for exchange in EXCHANGES:
        for symbol in SYMBOLS:
            for interval in INTERVALS:
                try:
                    logger.info(f"Fetching futures price history for {exchange} {symbol} {interval}")

                    data = client.get_futures_price_history(
                        exchange=exchange,
                        symbol=symbol,
                        interval=interval,
                        limit=LIMIT,
                        start_time=start_time,
                        end_time=end_time
                    )

                    if data:
                        # Process and insert data with duplicate checking
                        result = repo.upsert_futures_price_history(
                            exchange, symbol, interval, data
                        )
                        logger.info(
                            f"✅ futures_price_history[{exchange}:{symbol}:{interval}]: "
                            f"received={len(data)}, saved={result.get('futures_price_history', 0)}, duplicates={result.get('futures_price_history_duplicates', 0)}"
                        )
                        # Handle both old int format and new dict format for backward compatibility
                        if isinstance(result, dict):
                            summary["futures_price_history"] += result.get("futures_price_history", 0)
                            if result.get("futures_price_history_duplicates", 0) > 0:
                                summary["futures_price_history_duplicates"] = summary.get("futures_price_history_duplicates", 0) + result.get("futures_price_history_duplicates", 0)
                        else:
                            summary["futures_price_history"] += result
                    else:
                        logger.warning(f"No data returned for futures price history: {exchange} {symbol} {interval}")

                    summary["fetches"] += 1

                except Exception as e:
                    logger.warning(f"Error fetching futures price history for {exchange} {symbol} {interval}: {e}")
                    summary["errors"] += 1
                    summary["fetches"] += 1
                    continue

    logger.info(f"📦 Futures Price History pipeline completed. Total records saved: {summary['futures_price_history']}, duplicates={summary['futures_price_history_duplicates']} ✅")
    return summary


def _ensure_usdt_pairs(symbols: List[str]) -> List[str]:
    pairs: List[str] = []
    for symbol in symbols:
        upper = symbol.upper()
        if upper.endswith(("USDT", "USDC", "USD")):
            pairs.append(symbol)
        else:
            pairs.append(f"{symbol}USDT")
    return pairs
