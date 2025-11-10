# app/providers/coinglass/pipelines/bitcoin_vs_global_m2_growth.py
import logging
from typing import Any, Dict
from app.repositories.coinglass_repository import CoinglassRepository

logger = logging.getLogger(__name__)


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Bitcoin vs Global M2 Supply & Growth Pipeline
    Fetches historical data comparing Bitcoin price with global M2 money supply.
    """
    repo = CoinglassRepository(conn, logger)

    summary = {
        "bitcoin_vs_global_m2_growth": 0,
        "bitcoin_vs_global_m2_growth_duplicates": 0,
        "fetches": 0
    }

    try:
        rows = client.get_bitcoin_vs_global_m2_growth()
        if rows:
            result = repo.upsert_bitcoin_vs_global_m2_growth(rows)
            saved = result.get("bitcoin_vs_global_m2_growth", 0)
            duplicates = result.get("bitcoin_vs_global_m2_growth_duplicates", 0)

            logger.info(
                f"✅ bitcoin_vs_global_m2_growth: "
                f"received={len(rows)}, saved={saved}, duplicates={duplicates}"
            )
            summary["bitcoin_vs_global_m2_growth"] = saved
            summary["bitcoin_vs_global_m2_growth_duplicates"] = duplicates
        else:
            logger.info("⚠️ bitcoin_vs_global_m2_growth: No data (skipped)")
        summary["fetches"] += 1
    except Exception as e:
        logger.warning(f"⚠️ bitcoin_vs_global_m2_growth: Exception: {e} (skipped)")
        summary["fetches"] += 1

    logger.info(
        f"📦 Bitcoin vs Global M2 summary -> "
        f"saved={summary['bitcoin_vs_global_m2_growth']}, "
        f"duplicates={summary['bitcoin_vs_global_m2_growth_duplicates']}, "
        f"fetches={summary['fetches']}"
    )

    return summary
