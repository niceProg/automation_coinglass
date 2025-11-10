# app/providers/coinglass/pipelines/oi_exchange_list.py
import logging
from typing import Any, Dict, List
from app.repositories.coinglass_repository import CoinglassRepository

logger = logging.getLogger(__name__)


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    OI Exchange List Pipeline
    Cadence: Every 5-15 minutes (real-time data)

    Fetches current open interest data by exchange for each symbol
    """
    repo = CoinglassRepository(conn, logger)

    SYMBOLS = params.get("symbols", ["BTC", "ETH", "SOL", "XRP", "HYPE", "BNB", "DOGE"])

    summary = {
        "oi_exchange_list": 0,
        "fetches": 0
    }

    # OI Exchange List (Current OI by exchange)
    for symbol in SYMBOLS:
        try:
            rows = client.get_oi_exchange_list(symbol=symbol)
            if rows:
                saved = repo.upsert_oi_exchange_list(symbol, rows)
                logger.info(f"✅ oi_exchange_list[{symbol}]: saved={saved}")
                summary["oi_exchange_list"] += saved
            else:
                logger.info(f"⚠️ oi_exchange_list[{symbol}]: No data (skipped)")
        except Exception as e:
            logger.warning(f"⚠️ oi_exchange_list[{symbol}]: Exception: {e} (skipped)")
            continue
        finally:
            summary["fetches"] += 1

    logger.info(
        f"📦 OI Exchange List summary -> total_saved={summary['oi_exchange_list']} | "
        f"fetches={summary['fetches']}"
    )

    return summary