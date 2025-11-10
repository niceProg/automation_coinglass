# app/providers/coinglass/pipelines/long_short_ratio_global.py
import logging
from typing import Any, Dict, List
from app.repositories.coinglass_repository import CoinglassRepository

logger = logging.getLogger(__name__)


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Long/Short Ratio (Global Account) Pipeline
    Cadence: 15–60m
    Timeframes: 1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w

    Collects Global Account Ratio data from Coinglass API.
    """
    repo = CoinglassRepository(conn, logger)

    # LSR specific timeframes (medium frequency)
    TIMEFRAMES = params.get("timeframes", ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"])
    SYMBOLS = params.get("symbols", ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"])
    EXCHANGES = params.get("exchanges", ["Binance", "Bybit"])

    summary = {
        "lsr_global_account_ratio": 0,
        "lsr_global_account_ratio_duplicates": 0,
        "lsr_global_account_fetches": 0
    }

    # Global Account Ratio
    for exchange in EXCHANGES:
        for symbol in SYMBOLS:
            pair = f"{symbol}USDT"
            for interval in TIMEFRAMES:
                try:
                    rows = client.get_lsr_global_account_ratio_history(
                        exchange=exchange, symbol=pair, interval=interval
                    )
                    if rows:
                        saved = repo.upsert_lsr_global_account_ratio(
                            exchange=exchange, pair=pair, interval=interval, rows=rows
                        )
                        logger.info(
                            f"✅ lsr_global_account[{exchange}:{pair}:{interval}]: "
                            f"received={len(rows)}, saved={saved.get('lsr_global_account_ratio', 0)}, duplicates={saved.get('lsr_global_account_ratio_duplicates', 0)}"
                        )
                        # Handle both old integer format and new dict format for backward compatibility
                        if isinstance(saved, dict):
                            summary["lsr_global_account_ratio"] += saved.get("lsr_global_account_ratio", 0)
                            # Add duplicate info to summary if available
                            if saved.get("lsr_global_account_ratio_duplicates", 0) > 0:
                                summary["lsr_global_account_ratio_duplicates"] = summary.get("lsr_global_account_ratio_duplicates", 0) + saved.get("lsr_global_account_ratio_duplicates", 0)
                        else:
                            summary["lsr_global_account_ratio"] += saved
                    else:
                        logger.info(
                            f"⚠️ lsr_global_account[{exchange}:{pair}:{interval}]: No data (skipped)"
                        )
                    summary["lsr_global_account_fetches"] += 1
                except Exception as e:
                    logger.warning(
                        f"⚠️ lsr_global_account[{exchange}:{pair}:{interval}]: Exception: {e} (skipped)"
                    )
                    summary["lsr_global_account_fetches"] += 1
                    continue

    if summary.get("lsr_global_account_ratio_duplicates", 0) > 0:
        logger.info(
            f"📦 Long/Short Ratio (Global Account) summary -> saved={summary['lsr_global_account_ratio']}, "
            f"duplicates={summary['lsr_global_account_ratio_duplicates']} (fetches:{summary['lsr_global_account_fetches']})"
        )
    else:
        logger.info(
            f"📦 Long/Short Ratio (Global Account) summary -> saved={summary['lsr_global_account_ratio']} (fetches:{summary['lsr_global_account_fetches']})"
        )

    return summary