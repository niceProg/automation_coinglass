# app/providers/coinglass/pipelines/hyperliquid_whale_alert.py
import logging
from typing import Any, Dict
from app.repositories.coinglass_repository import CoinglassRepository

logger = logging.getLogger(__name__)


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Hyperliquid Whale Alert Pipeline
    Fetches recent large position changes on Hyperliquid exchange.
    """
    repo = CoinglassRepository(conn, logger)

    summary = {
        "hyperliquid_whale_alert": 0,
        "hyperliquid_whale_alert_duplicates": 0,
        "fetches": 0
    }

    try:
        rows = client.get_hyperliquid_whale_alert()
        if rows:
            result = repo.upsert_hyperliquid_whale_alert(rows)
            saved = result.get("hyperliquid_whale_alert", 0)
            duplicates = result.get("hyperliquid_whale_alert_duplicates", 0)

            logger.info(
                f"✅ hyperliquid_whale_alert: "
                f"received={len(rows)}, saved={saved}, duplicates={duplicates}"
            )
            summary["hyperliquid_whale_alert"] = saved
            summary["hyperliquid_whale_alert_duplicates"] = duplicates
        else:
            logger.info("⚠️ hyperliquid_whale_alert: No data (skipped)")
        summary["fetches"] += 1
    except Exception as e:
        logger.warning(f"⚠️ hyperliquid_whale_alert: Exception: {e} (skipped)")
        summary["fetches"] += 1

    logger.info(
        f"📦 Hyperliquid Whale Alert summary -> "
        f"saved={summary['hyperliquid_whale_alert']}, "
        f"duplicates={summary['hyperliquid_whale_alert_duplicates']}, "
        f"fetches={summary['fetches']}"
    )

    return summary
