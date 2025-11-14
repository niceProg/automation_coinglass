# app/providers/coinglass/pipelines/funding_rate.py
import logging
from typing import Any, Dict, List
from app.repositories.coinglass_repository import CoinglassRepository

logger = logging.getLogger(__name__)


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Funding Rate Pipeline
    Cadence: Each funding interval (8 hours) + 1-hour snapshot
    Timeframes: 1h, 8h
    """
    repo = CoinglassRepository(conn, logger)

    # Funding rate specific timeframes
    TIMEFRAMES = params.get("timeframes", ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"])
    SYMBOLS = params.get("symbols", ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"])
    EXCHANGES = params.get("exchanges", ["Binance", "Bybit"])
    # LIMIT = params.get("limit", 1000)  # Removed - using API default

    summary = {
        "fr_history": 0,
        "fr_exchange_list": 0,
        "fr_history_fetches": 0
    }

    # 1) Funding Rate OHLC History
    for exchange in EXCHANGES:
        for symbol in SYMBOLS:
            pair = f"{symbol}USDT"
            for interval in TIMEFRAMES:
                try:
                    # Use time parameters if available for historical data retrieval
                    start_time = params.get("start_time")
                    end_time = params.get("end_time")

                    rows = client.get_fr_history(
                        exchange=exchange, symbol=pair, interval=interval,
                        start_time=start_time, end_time=end_time
                    )
                    if rows:
                        saved = repo.upsert_fr_history(
                            exchange=exchange, pair=pair, interval=interval, rows=rows
                        )
                        logger.info(
                            f"✅ fr_history[{exchange}:{pair}:{interval}]: "
                            f"received={len(rows)}, saved={saved}"
                        )
                        # Handle both old integer format and new dict format for backward compatibility
                        if isinstance(saved, dict):
                            summary["fr_history"] += saved.get("fr_history", 0)
                            # Add duplicate and filtered info to summary if available
                            if saved.get("fr_history_duplicates", 0) > 0:
                                summary["fr_history_duplicates"] = summary.get("fr_history_duplicates", 0) + saved.get("fr_history_duplicates", 0)
                            if saved.get("fr_history_filtered", 0) > 0:
                                summary["fr_history_filtered"] = summary.get("fr_history_filtered", 0) + saved.get("fr_history_filtered", 0)
                        else:
                            summary["fr_history"] += saved
                    else:
                        logger.info(
                            f"⚠️ fr_history[{exchange}:{pair}:{interval}]: No data (skipped)"
                        )
                    summary["fr_history_fetches"] += 1
                except Exception as e:
                    logger.warning(
                        f"⚠️ fr_history[{exchange}:{pair}:{interval}]: Exception: {e} (skipped)"
                    )
                    summary["fr_history_fetches"] += 1
                    continue

    # 2) Exchange List (Current funding rates)
    for symbol in SYMBOLS:
        try:
            data = client.get_fr_exchange_list(symbol=symbol)
            if data and isinstance(data, list) and len(data) > 0:
                # API returns a list with one item containing stablecoin_margin_list and token_margin_list
                item = data[0]
                saved = repo.upsert_fr_exchange_list(symbol, item)
                logger.info(f"✅ fr_exchange_list[{symbol}]: saved={saved}")
                summary["fr_exchange_list"] += saved
            else:
                logger.info(f"⚠️ fr_exchange_list[{symbol}]: No data (skipped)")
        except Exception as e:
            logger.warning(f"⚠️ fr_exchange_list[{symbol}]: Exception: {e} (skipped)")
            continue

    total_saved = summary["fr_history"] + summary["fr_exchange_list"]
    logger.info(
        f"📦 Funding Rate summary -> total_saved={total_saved} | "
        f"fr_history:{summary['fr_history']} (fetches:{summary['fr_history_fetches']}), "
        f"fr_exchange_list:{summary['fr_exchange_list']}"
    )

    return summary
