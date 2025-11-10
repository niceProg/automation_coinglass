"""Spot Large Orderbook History Pipeline"""

import logging
from typing import Any, Dict, List
from datetime import datetime, timedelta
from app.repositories.coinglass_repository import CoinglassRepository
from app.core.config import Settings

logger = logging.getLogger(__name__)
settings = Settings()


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Spot Large Orderbook History Pipeline
    Cadence: Every 10 minutes
    Endpoint: /api/spot/orderbook/large-limit-order-history

    Retrieves historical data for large limit orders in spot markets,
    including order details, execution status, and timing information.
    """
    repo = CoinglassRepository(conn, logger)

    # Pipeline parameters
    EXCHANGES = params.get("exchanges", ["Binance", "Bybit"])  # Only Binance and Bybit supported
    SYMBOLS = params.get("symbols", ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "HYPEUSDT", "BNBUSDT", "DOGEUSDT"])

    # Order states: 1 = In Progress, 2 = Finish, 3 = Revoke
    ORDER_STATES = params.get("states", ["1", "2", "3"])  # Default to all states

    # Calculate time range for historical data
    HOURS_BACK = params.get("hours_back", 24)  # 24 hours back for good data coverage
    start_time = params.get("start_time")
    if not start_time:
        start_time = int((datetime.now() - timedelta(hours=HOURS_BACK)).timestamp() * 1000)

    end_time = params.get("end_time", int(datetime.now().timestamp() * 1000))

    summary = {
        "large_orderbook_history": 0,
        "fetches": 0,
        "errors": 0
    }

    logger.info(f"Starting Spot Large Orderbook History pipeline for exchanges: {EXCHANGES}")

    for exchange in EXCHANGES:
        for symbol in SYMBOLS:
            for state in ORDER_STATES:
                try:
                    logger.info(f"Fetching large orderbook history for {exchange} {symbol} state={state}")

                    data = client.get_spot_large_orderbook_history(
                        exchange=exchange,
                        symbol=symbol,
                        start_time=start_time,
                        end_time=end_time,
                        state=state
                    )

                    if data:
                        # Process and insert data
                        repo.insert_spot_large_orderbook_history(exchange, symbol, state, data)
                        summary["large_orderbook_history"] += len(data)
                        logger.info(f"Inserted {len(data)} large orderbook records for {exchange} {symbol} state={state}")
                    else:
                        logger.info(f"No large orderbook data available for {exchange} {symbol} state={state}")

                    summary["fetches"] += 1

                except Exception as e:
                    logger.error(f"Error fetching large orderbook history for {exchange} {symbol} state={state}: {e}")
                    summary["errors"] += 1

    logger.info(f"Spot Large Orderbook History pipeline completed: {summary}")
    return summary