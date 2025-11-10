"""Spot Large Orderbook Pipeline"""

import logging
from typing import Any, Dict, List
from datetime import datetime, timedelta
from app.repositories.coinglass_repository import CoinglassRepository
from app.core.config import Settings

logger = logging.getLogger(__name__)
settings = Settings()


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Spot Large Orderbook Pipeline
    Cadence: Every 1 minute (real-time data)
    Endpoint: /api/spot/orderbook/large-limit-order

    Retrieves current large limit orders in spot markets,
    including order details, current status, and execution information.
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters
    EXCHANGES = params.get("exchanges", ["Binance", "Bybit"])  # Only Binance and Bybit supported
    SYMBOLS = params.get("symbols", ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "HYPEUSDT", "BNBUSDT", "DOGEUSDT"])

    summary = {
        "large_orderbook": 0,
        "fetches": 0,
        "errors": 0
    }

    logger.info(f"Starting Spot Large Orderbook pipeline for exchanges: {EXCHANGES}")

    for exchange in EXCHANGES:
        for symbol in SYMBOLS:
            try:
                logger.info(f"Fetching large orderbook for {exchange} {symbol}")

                data = client.get_spot_large_orderbook(
                    exchange=exchange,
                    symbol=symbol
                )

                if data:
                    # Process and insert data
                    repo.insert_spot_large_orderbook(exchange, symbol, data)
                    summary["large_orderbook"] += len(data)
                    logger.info(f"Inserted {len(data)} large orderbook records for {exchange} {symbol}")
                else:
                    logger.info(f"No large orderbook data available for {exchange} {symbol}")

                summary["fetches"] += 1

            except Exception as e:
                logger.error(f"Error fetching large orderbook for {exchange} {symbol}: {e}")
                summary["errors"] += 1

    logger.info(f"Spot Large Orderbook pipeline completed: {summary}")
    return summary