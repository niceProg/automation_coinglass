# app/providers/coinglass/pipelines/open_interest.py
import logging
from typing import Any, Dict, List
from app.repositories.coinglass_repository import CoinglassRepository

logger = logging.getLogger(__name__)


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Open Interest Pipeline
    Cadence: 1–5m (or at least 15m)
    Timeframes: 1m, 5m, 15m
    """
    repo = CoinglassRepository(conn, logger)

    # OI specific timeframes (high frequency)
    TIMEFRAMES = params.get("timeframes", ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"])
    SYMBOLS = params.get("symbols", ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"])
    EXCHANGES = params.get("exchanges", ["Binance", "Bybit"])
    # LIMIT = params.get("limit", 1000)  # Removed - using API default
    UNIT = params.get("unit", "usd")

    summary = {
        "oi_history": 0,
                "oi_aggregated_history": 0,
        "oi_history_fetches": 0,
        "oi_aggregated_fetches": 0
    }

    # 1) OI OHLC History (DISABLED)
    # for exchange in EXCHANGES:
    #     for symbol in SYMBOLS:
    #         pair = f"{symbol}USDT"
    #         for interval in TIMEFRAMES:
    #             try:
    #                 rows = client.get_oi_history(
    #                     exchange=exchange, symbol=pair, interval=interval,
    #                     unit=UNIT
    #                 )
    #                 if rows:
    #                     saved = repo.upsert_oi_history(
    #                         exchange=exchange, pair=pair, interval=interval,
    #                         rows=rows, unit=UNIT
    #                     )
    #                     logger.info(
    #                         f"✅ oi_history[{exchange}:{pair}:{interval}]: "
    #                         f"received={len(rows)}, saved={saved}"
    #                     )
    #                     summary["oi_history"] += saved
    #                 else:
    #                     logger.info(
    #                         f"⚠️ oi_history[{exchange}:{pair}:{interval}]: No data (skipped)"
    #                     )
    #                 summary["oi_history_fetches"] += 1
    #             except Exception as e:
    #                 logger.warning(
    #                     f"⚠️ oi_history[{exchange}:{pair}:{interval}]: Exception: {e} (skipped)"
    #                 )
    #                 summary["oi_history_fetches"] += 1
    #                 continue

    
    # 2) OI Aggregated History (OHLC aggregated data across exchanges)
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

                rows = client.get_oi_aggregated_history(
                    symbol=symbol, interval=interval,
                    unit=UNIT, **time_params
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

    total_saved = summary["oi_aggregated_history"]
    logger.info(
        f"📦 Open Interest summary -> total_saved={total_saved} | "
        f"oi_history:DISABLED (fetches:{summary['oi_history_fetches']}), "
        f"oi_aggregated_history:{summary['oi_aggregated_history']} (fetches:{summary['oi_aggregated_fetches']})"
    )

    return summary
