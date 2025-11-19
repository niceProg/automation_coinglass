import logging
from typing import Any, Dict, List
from datetime import datetime, timedelta
from app.repositories.coinglass_repository import CoinglassRepository

logger = logging.getLogger(__name__)


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Spot Price History Pipeline
    Cadence: Every 5 minutes (real-time data)
    Endpoint: /api/spot/price/history

    Retrieves historical price OHLC data for specific trading pairs,
    including open, high, low, close prices and trading volume.
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters - mandatory requirements
    EXCHANGES = params.get("exchanges", ["Binance", "Bybit"])  # Binance and Bybit as requested
    SYMBOLS = params.get("symbols", ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "HYPEUSDT", "BNBUSDT", "DOGEUSDT"])  # All mandatory pairs
    INTERVALS = params.get("intervals", ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"])  # All supported intervals
    # LIMIT = params.get("limit", 100)  # Removed - using API default

    summary = {
        "spot_price_history": 0,
        "spot_price_history_duplicates": 0,
        "fetches": 0
    }

    logger.info(f"Starting Spot Price History pipeline for symbols: {SYMBOLS}")

    for exchange in EXCHANGES:
        for symbol in SYMBOLS:
            for interval in INTERVALS:
                try:
                    logger.info(f"Fetching spot price history for {exchange} {symbol} {interval}")

                    # Pass time parameters if available
                    time_params = {}
                    if "start_time" in params:
                        time_params["start_time"] = params["start_time"]
                    if "end_time" in params:
                        time_params["end_time"] = params["end_time"]

                    if time_params:
                        logger.info(f"Using time parameters: {time_params}")

                    rows = client.get_spot_price_history(
                        exchange=exchange,
                        symbol=symbol,
                        interval=interval,
                        **time_params
                    )

                    if rows:
                        saved = repo.upsert_spot_price_history(
                            exchange, symbol, interval, rows
                        )
                        logger.info(
                            f"✅ spot_price_history[{exchange}:{symbol}:{interval}]: "
                            f"received={len(rows)}, saved={saved.get('spot_price_history', 0)}, duplicates={saved.get('spot_price_history_duplicates', 0)}"
                        )
                        # Handle both old int format and new dict format for backward compatibility
                        if isinstance(saved, dict):
                            summary["spot_price_history"] += saved.get("spot_price_history", 0)
                            if saved.get("spot_price_history_duplicates", 0) > 0:
                                summary["spot_price_history_duplicates"] = summary.get("spot_price_history_duplicates", 0) + saved.get("spot_price_history_duplicates", 0)
                        else:
                            summary["spot_price_history"] += saved
                    else:
                        logger.warning(f"No data returned for spot price history: {exchange} {symbol} {interval}")

                    summary["fetches"] += 1

                except Exception as e:
                    logger.warning(f"Error fetching spot price history for {exchange} {symbol}: {e}")
                    summary["fetches"] += 1
                    continue

    logger.info(f"📦 Spot Price History pipeline completed. Total records saved: {summary['spot_price_history']}, duplicates={summary['spot_price_history_duplicates']} ✅")
    return summary