"""Spot Orderbook Pipeline"""

import logging
from typing import Any, Dict, List
from datetime import datetime, timedelta
from app.repositories.coinglass_repository import CoinglassRepository
from app.core.config import Settings

logger = logging.getLogger(__name__)
settings = Settings()


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Spot Orderbook Pipeline
    Cadence: Every 5 seconds (real-time data)
    Endpoint: /api/spot/orderbook/history

    Retrieves spot orderbook data for specific exchanges and pairs,
    including order book depth at various percentage ranges.
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters
    EXCHANGES = params.get("exchanges", ["Binance", "Bybit"])  # Only Binance and Bybit
    SYMBOLS = params.get("symbols", ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "HYPEUSDT", "BNBUSDT", "DOGEUSDT"])  # All mandatory pairs
    INTERVALS = params.get("intervals", ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"])  # All supported intervals
    RANGES = params.get("ranges", ["0.25", "0.5"])  # Depth percentages

    # Calculate start_time for historical data if not provided
    HOURS_BACK = params.get("hours_back", 2)  # 2 hours back for more data availability
    start_time = params.get("start_time")
    if not start_time:
        start_time = int((datetime.now() - timedelta(hours=HOURS_BACK)).timestamp() * 1000)

    summary = {
        "spot_orderbook": 0,
        "spot_orderbook_duplicates": 0,
        "fetches": 0
    }

    logger.info(f"Starting Spot Orderbook pipeline for exchanges: {EXCHANGES}")

    for exchange in EXCHANGES:
        for symbol in SYMBOLS:
            for interval in INTERVALS:
                for range_percent in RANGES:
                    try:
                        logger.info(f"Fetching spot orderbook for {exchange} {symbol} {interval} range={range_percent}")
                        rows = client.get_spot_orderbook_history(
                            exchange=exchange,
                            pair=symbol,
                            interval=interval,
                            range_percent=range_percent,
                            start_time=start_time
                        )

                        if rows:
                            saved = repo.upsert_spot_orderbook_history(
                                exchange, symbol, interval, range_percent, rows
                            )
                            logger.info(
                                f"✅ spot_orderbook[{exchange}:{symbol}:{interval}:{range_percent}]: "
                                f"received={len(rows)}, saved={saved.get('spot_orderbook', 0)}, duplicates={saved.get('spot_orderbook_duplicates', 0)}"
                            )
                            # Handle both old int format and new dict format for backward compatibility
                            if isinstance(saved, dict):
                                summary["spot_orderbook"] += saved.get("spot_orderbook", 0)
                                if saved.get("spot_orderbook_duplicates", 0) > 0:
                                    summary["spot_orderbook_duplicates"] = summary.get("spot_orderbook_duplicates", 0) + saved.get("spot_orderbook_duplicates", 0)
                            else:
                                summary["spot_orderbook"] += saved
                        else:
                            logger.warning(f"No data returned for spot orderbook: {exchange} {symbol} {interval}")

                        summary["fetches"] += 1

                    except Exception as e:
                        logger.warning(f"Error fetching spot orderbook for {exchange} {symbol}: {e}")
                        summary["fetches"] += 1
                        continue

    logger.info(f"📦 Spot Orderbook pipeline completed. Total records saved: {summary['spot_orderbook']}, duplicates={summary['spot_orderbook_duplicates']} ✅")
    return summary