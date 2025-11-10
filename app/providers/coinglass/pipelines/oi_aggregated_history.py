# app/providers/coinglass/pipelines/oi_aggregated_history.py
import logging
from typing import Any, Dict, List
from app.repositories.coinglass_repository import CoinglassRepository

logger = logging.getLogger(__name__)


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    OI Aggregated History Pipeline
    Cadence: 1–5m (or at least 15m)
    Timeframes: 1m, 5m, 15m

    Fetches OI OHLC data aggregated across exchanges by symbol and interval
    """
    repo = CoinglassRepository(conn, logger)

    # OI specific timeframes (high frequency)
    TIMEFRAMES = params.get("timeframes", ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"])
    SYMBOLS = params.get("symbols", ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"])
    UNIT = params.get("unit", "usd")

    summary = {
        "oi_aggregated_history": 0,
        "oi_aggregated_fetches": 0
    }

    # OI Aggregated History (OHLC aggregated data across exchanges)
    for symbol in SYMBOLS:
        for interval in TIMEFRAMES:
            try:
                rows = client.get_oi_aggregated_history(
                    symbol=symbol, interval=interval,
                    unit=UNIT
                )
                if rows:
                    saved = repo.upsert_oi_aggregated_history(
                        symbol=symbol, interval=interval,
                        rows=rows, unit=UNIT
                    )
                    logger.info(
                        f"✅ oi_aggregated_history[{symbol}:{interval}]: "
                        f"received={len(rows)}, saved={saved}"
                    )
                    summary["oi_aggregated_history"] += saved
                else:
                    logger.info(
                        f"⚠️ oi_aggregated_history[{symbol}:{interval}]: No data (skipped)"
                    )
                summary["oi_aggregated_fetches"] += 1
            except Exception as e:
                logger.warning(
                    f"⚠️ oi_aggregated_history[{symbol}:{interval}]: Exception: {e} (skipped)"
                )
                summary["oi_aggregated_fetches"] += 1
                continue

    logger.info(
        f"📦 OI Aggregated History summary -> total_saved={summary['oi_aggregated_history']} | "
        f"fetches={summary['oi_aggregated_fetches']}"
    )

    return summary