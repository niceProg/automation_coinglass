# app/providers/coinglass/pipelines/fear_greed_index.py
import logging
from typing import Any, Dict
from app.repositories.coinglass_repository import CoinglassRepository

logger = logging.getLogger(__name__)


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fear & Greed Index Pipeline
    Fetches historical fear and greed index data.

    Data Structure:
    - Main table: cg_fear_greed_index (one record per fetch)
    - Child table: cg_fear_greed_index_data_list (multiple index values linked to parent)
    """
    repo = CoinglassRepository(conn, logger)

    summary = {
        "fear_greed_index": 0,
        "fear_greed_index_duplicates": 0,
        "fear_greed_index_data_list": 0,
        "fear_greed_index_data_list_duplicates": 0,
        "fetches": 0
    }

    try:
        data = client.get_fear_greed_history()
        if data:
            # Log sample data for debugging
            data_list = data.get("data_list", [])
            if data_list:
                logger.info(
                    f"Sample Fear & Greed Index data: "
                    f"total_values={len(data_list)}, "
                    f"first_5={data_list[:5]}, "
                    f"last_5={data_list[-5:]}"
                )

            result = repo.upsert_fear_greed_index(data)
            saved = result.get("fear_greed_index", 0)
            duplicates = result.get("fear_greed_index_duplicates", 0)
            data_list_saved = result.get("fear_greed_index_data_list", 0)
            data_list_duplicates = result.get("fear_greed_index_data_list_duplicates", 0)

            logger.info(
                f"✅ fear_greed_index: saved={saved}, duplicates={duplicates} | "
                f"data_list: saved={data_list_saved}, duplicates={data_list_duplicates}"
            )
            summary["fear_greed_index"] = saved
            summary["fear_greed_index_duplicates"] = duplicates
            summary["fear_greed_index_data_list"] = data_list_saved
            summary["fear_greed_index_data_list_duplicates"] = data_list_duplicates
        else:
            logger.info("⚠️ fear_greed_index: No data (skipped)")
        summary["fetches"] += 1
    except Exception as e:
        logger.warning(f"⚠️ fear_greed_index: Exception: {e} (skipped)")
        summary["fetches"] += 1

    logger.info(
        f"📦 Fear & Greed Index summary -> "
        f"parent: saved={summary['fear_greed_index']}, duplicates={summary['fear_greed_index_duplicates']} | "
        f"children: saved={summary['fear_greed_index_data_list']}, duplicates={summary['fear_greed_index_data_list_duplicates']} | "
        f"fetches={summary['fetches']}"
    )

    return summary
