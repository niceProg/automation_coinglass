# app/providers/coinglass/pipelines/futures_basis.py
import logging
from typing import Any, Dict, List
from app.repositories.coinglass_repository import CoinglassRepository

logger = logging.getLogger(__name__)


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Futures Basis History Pipeline
    Cadence: Real-time for all API plans
    Timeframes: 1m, 3m, 5m, 15m, 30m, 1h, 4h, 6h, 8h, 12h, 1d, 1w

    Collects futures basis data for trading pairs on exchanges.
    Basis represents the price difference between futures and spot prices.
    """
    repo = CoinglassRepository(conn, logger)

    # Futures basis specific timeframes
    TIMEFRAMES = params.get("timeframes", ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "12h", "1d", "1w"])
    PAIRS = params.get("pairs", ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "HYPEUSDT", "BNBUSDT", "DOGEUSDT"])
    EXCHANGES = params.get("exchanges", ["Binance", "Bybit"])

    summary = {
        "futures_basis": 0,
        "futures_basis_duplicates": 0,
        "futures_basis_fetches": 0
    }

    # Futures Basis History
    for exchange in EXCHANGES:
        for pair in PAIRS:
            for interval in TIMEFRAMES:
                try:
                    rows = client.get_futures_basis_history(
                        exchange=exchange, symbol=pair, interval=interval
                    )
                    if rows:
                        saved = repo.upsert_futures_basis_history(
                            exchange=exchange, pair=pair, interval=interval, rows=rows
                        )
                        logger.info(
                            f"✅ futures_basis[{exchange}:{pair}:{interval}]: "
                            f"received={len(rows)}, saved={saved.get('futures_basis', 0)}, duplicates={saved.get('futures_basis_duplicates', 0)}"
                        )
                        # Handle both old int format and new dict format for backward compatibility
                        if isinstance(saved, dict):
                            summary["futures_basis"] += saved.get("futures_basis", 0)
                            if saved.get("futures_basis_duplicates", 0) > 0:
                                summary["futures_basis_duplicates"] = summary.get("futures_basis_duplicates", 0) + saved.get("futures_basis_duplicates", 0)
                        else:
                            summary["futures_basis"] += saved
                    else:
                        logger.info(
                            f"⚠️ futures_basis[{exchange}:{pair}:{interval}]: No data (skipped)"
                        )
                    summary["futures_basis_fetches"] += 1
                except Exception as e:
                    logger.warning(
                        f"⚠️ futures_basis[{exchange}:{pair}:{interval}]: Exception: {e} (skipped)"
                    )
                    summary["futures_basis_fetches"] += 1
                    continue

    logger.info(
        f"📦 Futures Basis summary -> total_saved={summary['futures_basis']}, duplicates={summary['futures_basis_duplicates']} | "
        f"futures_basis:{summary['futures_basis']} (fetches:{summary['futures_basis_fetches']})"
    )

    return summary