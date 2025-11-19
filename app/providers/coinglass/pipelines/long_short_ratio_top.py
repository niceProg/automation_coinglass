# app/providers/coinglass/pipelines/long_short_ratio_top.py
import logging
from typing import Any, Dict, List
from app.repositories.coinglass_repository import CoinglassRepository

logger = logging.getLogger(__name__)


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Long/Short Ratio - Top Account History Pipeline
    Endpoint: /api/futures/top-long-short-account-ratio/history
    Table: cg_long_short_top_account_ratio_history

    Cadence: 15–60m
    Timeframes: 1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w

    Collects historical data for the long/short ratio of top accounts.
    Shows the position distribution of the largest traders.

    Response fields:
    - time: Timestamp
    - top_account_long_percent: Long position % of top accounts
    - top_account_short_percent: Short position % of top accounts
    - top_account_long_short_ratio: Long/Short ratio of top accounts
    """
    repo = CoinglassRepository(conn, logger)

    # LSR specific timeframes (medium frequency)
    TIMEFRAMES = params.get("timeframes", ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"])
    SYMBOLS = params.get("symbols", ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"])
    EXCHANGES = params.get("exchanges", ["Binance", "Bybit"])

    summary = {
        "lsr_top_account_ratio": 0,
        "lsr_top_account_ratio_duplicates": 0,
        "lsr_top_account_fetches": 0
    }

    # Top Account Ratio
    for exchange in EXCHANGES:
        for symbol in SYMBOLS:
            pair = f"{symbol}USDT"
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

                    rows = client.get_lsr_top_account_ratio_history(
                        exchange=exchange, symbol=pair, interval=interval, **time_params
                    )
                    if rows:
                        saved = repo.upsert_lsr_top_account_ratio(
                            exchange=exchange, pair=pair, interval=interval, rows=rows
                        )
                        logger.info(
                            f"✅ lsr_top_account[{exchange}:{pair}:{interval}]: "
                            f"received={len(rows)}, saved={saved.get('lsr_top_account_ratio', 0)}, duplicates={saved.get('lsr_top_account_ratio_duplicates', 0)}"
                        )
                        # Handle both old integer format and new dict format for backward compatibility
                        if isinstance(saved, dict):
                            summary["lsr_top_account_ratio"] += saved.get("lsr_top_account_ratio", 0)
                            # Add duplicate info to summary if available
                            if saved.get("lsr_top_account_ratio_duplicates", 0) > 0:
                                summary["lsr_top_account_ratio_duplicates"] = summary.get("lsr_top_account_ratio_duplicates", 0) + saved.get("lsr_top_account_ratio_duplicates", 0)
                        else:
                            summary["lsr_top_account_ratio"] += saved
                    else:
                        logger.info(
                            f"⚠️ lsr_top_account[{exchange}:{pair}:{interval}]: No data (skipped)"
                        )
                    summary["lsr_top_account_fetches"] += 1
                except Exception as e:
                    logger.warning(
                        f"⚠️ lsr_top_account[{exchange}:{pair}:{interval}]: Exception: {e} (skipped)"
                    )
                    summary["lsr_top_account_fetches"] += 1
                    continue

    if summary.get("lsr_top_account_ratio_duplicates", 0) > 0:
        logger.info(
            f"📦 Long/Short Ratio (Top Account) summary -> saved={summary['lsr_top_account_ratio']}, "
            f"duplicates={summary['lsr_top_account_ratio_duplicates']} (fetches:{summary['lsr_top_account_fetches']})"
        )
    else:
        logger.info(
            f"📦 Long/Short Ratio (Top Account) summary -> saved={summary['lsr_top_account_ratio']} (fetches:{summary['lsr_top_account_fetches']})"
        )

    return summary