import logging
from typing import Any, Dict, List
from app.repositories.coinglass_repository import CoinglassRepository

logger = logging.getLogger(__name__)


def run(conn, client, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Bitcoin ETF List Pipeline
    Cadence: Every 5 minutes (real-time data)
    Endpoint: /api/etf/bitcoin/list

    Retrieves a list of Bitcoin Exchange-Traded Funds (ETFs) with key status information
    including fund details, market data, and asset information.
    """
    repo = CoinglassRepository(conn, logger)

    summary = {
        "bitcoin_etf_list": 0,
        "bitcoin_etf_list_duplicates": 0,
        "bitcoin_etf_list_filtered": 0,
        "fetches": 0
    }

    logger.info("Starting Bitcoin ETF List pipeline")

    try:
        logger.info("Fetching Bitcoin ETF list")
        rows = client.get_etf_bitcoin_list()

        if rows:
            result = repo.upsert_bitcoin_etf_list(rows)
            saved = result.get("bitcoin_etf_list", 0)
            duplicates = result.get("bitcoin_etf_list_duplicates", 0)
            filtered = result.get("bitcoin_etf_list_filtered", 0)

            summary["bitcoin_etf_list"] += saved
            summary["bitcoin_etf_list_duplicates"] += duplicates
            summary["bitcoin_etf_list_filtered"] += filtered

            logger.info(
                f"✅ Bitcoin ETF list: received={len(rows)}, filtered={filtered}, "
                f"saved={saved}, duplicates={duplicates} ✅"
            )
        else:
            logger.warning("No data returned for Bitcoin ETF list")

        summary["fetches"] += 1

    except Exception as e:
        logger.warning(f"Error fetching Bitcoin ETF list: {e}")
        summary["fetches"] += 1

    logger.info(
        f"📦 Bitcoin ETF List pipeline completed. Total records saved: {summary['bitcoin_etf_list']} "
        f"(duplicates: {summary['bitcoin_etf_list_duplicates']}, filtered: {summary['bitcoin_etf_list_filtered']}) ✅"
    )
    return summary