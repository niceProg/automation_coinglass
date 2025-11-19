# app/providers/coinglass/pipelines/liquidation_aggregated.py
import logging
from typing import Any, Dict, List
from app.repositories.coinglass_repository import CoinglassRepository

logger = logging.getLogger(__name__)


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Liquidation Aggregated History Pipeline
    Endpoint: /api/futures/liquidation/aggregated-history
    Table: cg_liquidation_aggregated_history

    Cadence: 15–60m
    Timeframes: 1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w

    Collects aggregated liquidation data for coins across exchanges.

    Note: Skips records where both aggregated_long_liquidation_usd
    and aggregated_short_liquidation_usd are 0.00000000 or null.
    """
    repo = CoinglassRepository(conn, logger)

    # Liquidation aggregated specific timeframes
    TIMEFRAMES = params.get("timeframes", ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"])
    SYMBOLS = params.get("symbols", ["BTC", "ETH", "SOL"])
    EXCHANGE_LIST = params.get("exchange_list", "Binance,Bybit")

    summary = {
        "liquidation_aggregated": 0,
        "liquidation_aggregated_duplicates": 0,
        "liquidation_aggregated_fetches": 0
    }

    # Liquidation Aggregated History
    for symbol in SYMBOLS:
        for interval in TIMEFRAMES:
            try:
                # Pass time parameters if available
                time_params = {}
                if "start_time" in params:
                    time_params["start_time"] = params["start_time"]
                if "end_time" in params:
                    time_params["end_time"] = params["end_time"]

                if time_params:
                    logger.info(f"Using time parameters: {time_params}")

                rows = client.get_liquidation_aggregated_history(
                    exchange_list=EXCHANGE_LIST, symbol=symbol, interval=interval, **time_params
                )
                if rows:
                    result = repo.upsert_liquidation_aggregated_history(
                        symbol=symbol, interval=interval, rows=rows
                    )
                    saved = result.get("liquidation_aggregated", 0)
                    duplicates = result.get("liquidation_aggregated_duplicates", 0)

                    logger.info(
                        f"✅ liquidation_aggregated[{symbol}:{interval}]: "
                        f"received={len(rows)}, saved={saved}, duplicates={duplicates}"
                    )
                    summary["liquidation_aggregated"] += saved
                    summary["liquidation_aggregated_duplicates"] += duplicates
                else:
                    logger.info(
                        f"⚠️ liquidation_aggregated[{symbol}:{interval}]: No data (skipped)"
                    )
                summary["liquidation_aggregated_fetches"] += 1
            except Exception as e:
                logger.warning(
                    f"⚠️ liquidation_aggregated[{symbol}:{interval}]: Exception: {e} (skipped)"
                )
                summary["liquidation_aggregated_fetches"] += 1
                continue

    logger.info(
        f"📦 Liquidation Aggregated summary -> total_saved={summary['liquidation_aggregated']} | "
        f"liquidation_aggregated:{summary['liquidation_aggregated']} (duplicates:{summary['liquidation_aggregated_duplicates']}) "
        f"(fetches:{summary['liquidation_aggregated_fetches']})"
    )

    return summary